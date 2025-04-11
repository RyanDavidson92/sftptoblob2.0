# v5_sftp_to_blob.py
# ===================
# ✅ V5 adds client-specific logic for secure chrooted SFTP ingestion.
# 🎯 Key features:
# - Distinguishes between clientA/clientB by login + folder path
# - Uploads raw files prefixed with client name to "clientinvoicesraw"
# - Uploads transformed files (with clientid + controlno) to "clientinvoices-transformed-with-controlno-and-clientid-added"
# - Avoids duplicates using Azure Blob existence check
# - Increments a global controlno (starting at 1000) across all files
# this is a test 
import os
import paramiko
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables
load_dotenv()

# Azure Blob Storage config
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
RAW_CONTAINER = "clientinvoicesraw"
TRANSFORMED_CONTAINER = "clientinvoices-transformed-with-controlno-and-clientid-added"

# Client config
CLIENTS = {
    "clientA": {"user": os.getenv("SFTP_CLIENTA_USER"), "pass": os.getenv("SFTP_CLIENTA_PASS"), "id": 12659},
    "clientB": {"user": os.getenv("SFTP_CLIENTB_USER"), "pass": os.getenv("SFTP_CLIENTB_PASS"), "id": 12660},
}

# Initialize Azure Blob client
blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)
raw_container_client = blob_service_client.get_container_client(RAW_CONTAINER)
transformed_container_client = blob_service_client.get_container_client(TRANSFORMED_CONTAINER)

CONTROLNO_START = 1000

# Wrapper 1: Add metadata columns to the DataFrame
def add_controlno_and_clientid(df, controlno, clientid):
    df['controlno'] = controlno
    df['clientid'] = clientid
    return df

# Wrapper 2: Upload raw or transformed file to Blob Storage, checking for duplicates
def upload_to_blob(file_data, blob_name, is_transformed=False):
    blob_client = (transformed_container_client if is_transformed else raw_container_client).get_blob_client(blob_name)
    if not blob_client.exists():
        blob_client.upload_blob(file_data, overwrite=True)
        print(f"✅ Uploaded {'transformed' if is_transformed else 'raw'}: {blob_name}")
    else:
        print(f"⚠️ Skipped duplicate blob: {blob_name}")

# Wrapper 3: Full processing of one file (download, transform, upload raw + transformed)
def process_file(sftp, client_name, file_name, controlno):
    remote_path = f"/upload/{file_name}"
    file_data = BytesIO()
    sftp.getfo(remote_path, file_data)
    file_data.seek(0)

    df = pd.read_csv(file_data)
    df = add_controlno_and_clientid(df, controlno, CLIENTS[client_name]['id'])

    output_buffer = BytesIO()
    df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)

    transformed_name = f"{client_name.lower()}_transformed_{file_name}"
    upload_to_blob(output_buffer, transformed_name, is_transformed=True)

    file_data.seek(0)
    raw_name = f"{client_name.lower()}_{file_name}"
    upload_to_blob(file_data, raw_name, is_transformed=False)

    return controlno + 1

# Wrapper 4: Connect to a client folder and process all new (non-transformed) files
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

# Wrapper 5: Loop through all defined clients and initiate processing
def main():
    controlno = CONTROLNO_START
    for client_name in CLIENTS:
        controlno = handle_client(client_name, controlno)
    print("\n✅ All client files processed.")

if __name__ == "__main__":
    main()