import os
import paramiko
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables from .env file testtesttesttesttesttesttesttest
load_dotenv()

# SFTP credentials
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_DIR = "/uploads"

# Azure Blob Storage credentials
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

# Initialize BlobServiceClient for Azure Blob Storage
blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER)

def upload_to_blob(file_data, blob_name):
    """Upload a file directly to Azure Blob Storage"""
    blob_client = container_client.get_blob_client(blob_name)
    if not blob_client.exists():
        # Upload the file content directly to Blob Storage
        blob_client.upload_blob(file_data, overwrite=True)
        print(f"✅ Successfully uploaded {blob_name} to Azure Blob Storage.")
    else:
        print(f"⚠️ File {blob_name} already exists in Blob Storage. Skipping upload.")

def transfer_files_from_sftp_to_blob():
    """Stream files from SFTP to Azure Blob Storage"""
    try:
        # Connect to SFTP server
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

        sftp = paramiko.SFTPClient.from_transport(transport)

        # List files in the SFTP uploads directory
        sftp_files = sftp.listdir(SFTP_DIR)
        for file_name in sftp_files:
            remote_file_path = os.path.join(SFTP_DIR, file_name)
            
            # Open the remote file on SFTP
            file_data = BytesIO()
            sftp.getfo(remote_file_path, file_data)  # Stream the file directly into memory
            file_data.seek(0)  # Rewind file pointer to the beginning

            # Upload the streamed file to Azure Blob Storage
            upload_to_blob(file_data, file_name)

        # Close the SFTP connection
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"❌ Error during SFTP to Blob transfer: {e}")

if __name__ == "__main__":
    transfer_files_from_sftp_to_blob()
