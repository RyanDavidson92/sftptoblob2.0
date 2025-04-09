### This script parallelly dumps two files from SFTP VPS into azure blob. One not transformed goes to "clientinoicesraw", the other transforms and goes to "clientinvoices-transformed-with-controlno-and-clientid-added"
## This allows for solving for data lineage issues by keep an un-transformed file. 


import os
import paramiko
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables from .env file
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
raw_container_client = blob_service_client.get_container_client("clientinvoicesraw")  # Raw container
transformed_container_client = blob_service_client.get_container_client("clientinvoices-transformed-with-controlno-and-clientid-added")  # Transformed container

# Define static client ID
CLIENT_ID = 12659  # Static client ID

def add_controlno_and_clientid(df, controlno=1001):
    """
    Adds controlno and clientid to the DataFrame.

    Args:
    - df: DataFrame containing the data
    - controlno: The control number to assign to the rows (default is 1001)

    Returns:
    - The DataFrame with the added controlno and clientid columns
    """
    df['controlno'] = controlno  # Control number starts at 1001
    df['clientid'] = CLIENT_ID   # Static client ID (12659)
    return df

def upload_to_blob(file_data, blob_name, is_transformed=False):
    """
    Uploads a file directly to Azure Blob Storage.

    Args:
    - file_data: Data to be uploaded (as BytesIO)
    - blob_name: The name of the blob (file name in storage)
    - is_transformed: Boolean flag indicating if this is a transformed file

    This function checks if the file already exists in the container. 
    If it doesn't, it uploads the file.
    """
    blob_client = transformed_container_client.get_blob_client(blob_name) if is_transformed else raw_container_client.get_blob_client(blob_name)
    
    if not blob_client.exists():
        # Upload the file content directly to Blob Storage
        blob_client.upload_blob(file_data, overwrite=True)
        print(f"✅ Successfully uploaded {'transformed' if is_transformed else 'raw'} {blob_name} to Azure Blob Storage.")
    else:
        print(f"⚠️ {('Transformed' if is_transformed else 'Raw')} file {blob_name} already exists in Blob Storage. Skipping upload.")

def process_sftp_file(file_name, sftp, controlno):
    """
    Processes a single file from SFTP: downloads, transforms, and uploads it to the appropriate blob container.

    Args:
    - file_name: The name of the file to be processed
    - sftp: The active SFTP connection
    - controlno: The control number to assign to this file's data

    Returns:
    - The incremented controlno after processing the file
    """
    print(f"Processing file: {file_name}")  # Debug message for each file
    remote_file_path = os.path.join(SFTP_DIR, file_name)
    file_data = BytesIO()
    sftp.getfo(remote_file_path, file_data)  # Stream the file directly into memory
    file_data.seek(0)  # Rewind file pointer to the beginning

    # Load data into DataFrame
    df = pd.read_csv(file_data)

    # Add controlno and clientid columns
    df = add_controlno_and_clientid(df, controlno)

    # Save transformed data to Blob Storage (in the transformed container)
    output_buffer = BytesIO()
    df.to_csv(output_buffer, index=False)
    output_buffer.seek(0)  # Rewind buffer before uploading

    # Create a new file name for transformed files
    transformed_file_name = f"transformed_{file_name}"

    # Upload to transformed container
    upload_to_blob(output_buffer, transformed_file_name, is_transformed=True)

    # Upload the raw file (without transformation) to the raw container
    file_data.seek(0)  # Rewind file pointer to upload the raw version again
    upload_to_blob(file_data, file_name, is_transformed=False)

    return controlno + 1  # Increment controlno for the next file

def transfer_and_transform_files_from_sftp_to_blob():
    """
    Main function to transfer files from the SFTP server to Azure Blob Storage.
    This function processes each file, applies transformations, and uploads them to the appropriate containers.
    """
    controlno = 1001  # Start controlno at 1001

    try:
        # Connect to SFTP server
        print("Starting file transfer from SFTP to Blob...")  # Debug message
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # List files in the SFTP uploads directory
        sftp_files = sftp.listdir(SFTP_DIR)
        print(f"Found {len(sftp_files)} files to process.")  # Debug message for the number of files

        for file_name in sftp_files:
            if file_name.startswith("transformed"):
                print(f"Skipping transformed file: {file_name}")  # Debug message for skipped files
                continue  # Skip transformed files

            # Process and upload the file
            controlno = process_sftp_file(file_name, sftp, controlno)

        # Close the SFTP connection
        sftp.close()
        transport.close()

        print("File transfer and transformation completed!")  # Success message

    except Exception as e:
        print(f"❌ Error during file transfer: {e}")

if __name__ == "__main__":
    transfer_and_transform_files_from_sftp_to_blob()
