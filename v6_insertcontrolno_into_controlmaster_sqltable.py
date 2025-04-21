# v6_sftp_to_blob_with_controlmaster.py
# This version builds on V5 and also inserts clientid metadata into the control_master SQL table, with SCOPE_IDENTITY capture.

import os
import paramiko
import pandas as pd
import pyodbc
import hashlib
from io import BytesIO
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import pytz
import time 

# Load environment variables
load_dotenv()

# Azure Blob Storage config
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
RAW_CONTAINER = os.getenv("AZURE_RAW_CONTAINER")
TRANSFORMED_CONTAINER = os.getenv("AZURE_TRANSFORMED_CONTAINER")

# Client config
CLIENTS = {
    "clientA": {"user": os.getenv("SFTP_CLIENTA_USER"), "pass": os.getenv("SFTP_CLIENTA_PASS"), "id": 12659},
    "clientB": {"user": os.getenv("SFTP_CLIENTB_USER"), "pass": os.getenv("SFTP_CLIENTB_PASS"), "id": 12660},
}

# Initialize Azure Blob client
blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)
raw_container_client = blob_service_client.get_container_client(RAW_CONTAINER)
transformed_container_client = blob_service_client.get_container_client(TRANSFORMED_CONTAINER)

CONTROLNO_START = 999

def get_next_controlno_from_sql():
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('SQL_DRIVER')};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};"
        f"PWD={os.getenv('SQL_PASSWORD')}"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT ISNULL(MAX(ControlNo), 999) + 1 FROM control_master")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result

def add_controlno_and_clientid(df, controlno, clientid):
    df.insert(0, 'controlno', controlno)
    df.insert(1, 'clientid', clientid)
    return df

def upload_to_blob(file_data, blob_name, is_transformed=False):
    blob_client = (transformed_container_client if is_transformed else raw_container_client).get_blob_client(blob_name)
    if not blob_client.exists():
        blob_client.upload_blob(file_data, overwrite=True)
        print(f"✅ Uploaded {'transformed' if is_transformed else 'raw'}: {blob_name}")
    else:
        print(f"⚠️ Skipped duplicate blob: {blob_name}")

def insert_into_control_master(clientid, filename, recordcount, file_bytes):
    try:
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        est = pytz.timezone('US/Eastern')
        load_timestamp = datetime.now(est).strftime('%Y-%m-%d %H:%M:%S')

        conn = pyodbc.connect(
            f"DRIVER={os.getenv('SQL_DRIVER')};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USERNAME')};"
            f"PWD={os.getenv('SQL_PASSWORD')}"
        )
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM control_master WHERE FileName = ? AND ClientID = ?", (filename, clientid))
        if cursor.fetchone():
            print(f"⚠️ Entry for {filename} and ClientID {clientid} already exists in control_master. Skipping insert.")
            conn.close()
            return

        cursor.execute("""
            INSERT INTO control_master (ClientID, FileName, RecordCount, LoadTimestamp, SourceSystem, FileHash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            clientid,
            filename,
            recordcount,
            load_timestamp,
            "pipeline transfer",
            file_hash
        ))

        cursor.execute("SELECT SCOPE_IDENTITY();")
        new_controlno = cursor.fetchone()[0]
        print(f"✅ SQL generated ControlNo: {new_controlno}")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Failed to insert or fetch ControlNo: {e}")

def process_file(sftp, client_name, file_name, controlno):
    remote_path = f"/upload/{file_name}"
    file_data = BytesIO()
    sftp.getfo(remote_path, file_data)
    file_data.seek(0)

    df = pd.read_csv(file_data, encoding='ISO-8859-1')
    df = add_controlno_and_clientid(df, controlno, CLIENTS[client_name]['id'])

    recordcount = len(df)
    file_data.seek(0)
    insert_into_control_master(
        clientid=CLIENTS[client_name]['id'],
        filename=file_name,
        recordcount=recordcount,
        file_bytes=file_data.read()
    )

    file_data.seek(0)
    output_buffer = BytesIO()
    df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    transformed_name = f"{client_name.lower()}_transformed_{timestamp}_{file_name}"
    upload_to_blob(output_buffer, transformed_name, is_transformed=True)

    file_data.seek(0)
    raw_name = f"{client_name.lower()}_{file_name}"
    upload_to_blob(file_data, raw_name, is_transformed=False)

    return controlno + 1

def handle_client(client_name, controlno):
    print(f"\n🔄 Connecting to {client_name}...")
    transport = paramiko.Transport((os.getenv("SFTP_HOST"), int(os.getenv("SFTP_PORT"))))
    transport.connect(username=CLIENTS[client_name]['user'], password=CLIENTS[client_name]['pass'])
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        files = sftp.listdir("/upload")
        for file_name in files:
            if file_name.startswith("transformed"):
                continue
            blob_name = f"{client_name.lower()}_{file_name}"
            if not raw_container_client.get_blob_client(blob_name).exists():
                controlno = process_file(sftp, client_name, file_name, controlno)
            else:
                print(f"🔁 Already processed: {blob_name}")
    finally:
        sftp.close()
        transport.close()

    return controlno

def wake_up_sql():
    try:
        print("🔌 Warming up SQL Server...")
        conn = pyodbc.connect(
            f"DRIVER={os.getenv('SQL_DRIVER')};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USERNAME')};"
            f"PWD={os.getenv('SQL_PASSWORD')}"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE();")
        cursor.fetchone()
        conn.close()
        print("✅ SQL Server is awake. Proceeding...")
    except Exception as e:
        print(f"⚠️ Wake-up query failed: {e}")
        print("⏳ Waiting 30 seconds before retrying...")
        time.sleep(30)
        conn = pyodbc.connect(
            f"DRIVER={os.getenv('SQL_DRIVER')};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USERNAME')};"
            f"PWD={os.getenv('SQL_PASSWORD')}"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE();")
        cursor.fetchone()
        conn.close()
        print("✅ SQL Server is now responsive.")

def main():
    wake_up_sql()
    controlno = get_next_controlno_from_sql()
    for client_name in CLIENTS:
        controlno = handle_client(client_name, controlno)
    print("\n✅ All client files processed.")

if __name__ == "__main__":
    main()
