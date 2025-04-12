# v5_sftp_to_blob.py
# ===================
# ✅ V5 adds client-specific logic for secure chrooted SFTP ingestion.
# 🎯 Key features:
# - Distinguishes between clientA/clientB by login + folder path
# - Uploads raw files prefixed with client name to "clientinvoicesraw"
# - Uploads transformed files (with clientid + controlno) to "clientinvoices-transformed-with-controlno-and-clientid-added"
# - Avoids duplicates using Azure Blob existence check
# - Increments a global controlno (starting at 1000) across all files


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

BATCHNUMBER_START = 1000

# Wrapper 1: Adds batchnumber and clientid as the first two columns in the DataFrame
def add_batchnumber_and_clientid(df, batchnumber, clientid):
    df.insert(0, 'batchnumber', batchnumber)
    df.insert(1, 'clientid', clientid)
    return df

# Wrapper 2: Uploads raw or transformed file data to the correct Azure Blob container
def upload_to_blob(file_data, blob_name, is_transformed=False):
    blob_client = (transformed_container_client if is_transformed else raw_container_client).get_blob_client(blob_name)
    if not blob_client.exists():
        blob_client.upload_blob(file_data, overwrite=True)
        print(f"✅ Uploaded {'transformed' if is_transformed else 'raw'}: {blob_name}")
    else:
        print(f"⚠️ Skipped duplicate blob: {blob_name}")

# Wrapper 3: Downloads a file from SFTP, transforms it, and uploads both versions to Blob Storage
def process_file(sftp, client_name, file_name, batchnumber):
    remote_path = f"/upload/{file_name}"
    file_data = BytesIO()
    sftp.getfo(remote_path, file_data)
    file_data.seek(0)

    df = pd.read_csv(file_data)
    df = add_batchnumber_and_clientid(df, batchnumber, CLIENTS[client_name]['id'])

    output_buffer = BytesIO()
    df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)

    transformed_name = f"{client_name.lower()}_transformed_{file_name}"
    upload_to_blob(output_buffer, transformed_name, is_transformed=True)

    file_data.seek(0)
    raw_name = f"{client_name.lower()}_{file_name}"
    upload_to_blob(file_data, raw_name, is_transformed=False)

    return batchnumber + 1

# Wrapper 4: Connects to each client's SFTP, processes new files, and skips previously processed ones
def handle_client(client_name, batchnumber):
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
                batchnumber = process_file(sftp, client_name, file_name, batchnumber)
            else:
                print(f"🔁 Already processed: {blob_name}")
    finally:
        sftp.close()
        transport.close()

    return batchnumber

# Wrapper 5: Main entry point that iterates through all clients and processes their files
def main():
    batchnumber = BATCHNUMBER_START
    for client_name in CLIENTS:
        batchnumber = handle_client(client_name, batchnumber)
    print("\n✅ All client files processed.")

if __name__ == "__main__":
    main()
