# admin_wipe_blob_containers.py
# ⚠️ Admin use only: This script deletes ALL contents from both raw and transformed blob containers

import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Blob config
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
RAW_CONTAINER = os.getenv("AZURE_RAW_CONTAINER")
TRANSFORMED_CONTAINER = os.getenv("AZURE_TRANSFORMED_CONTAINER")

# Initialize client
blob_service_client = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)

# Delete all blobs in a container
def wipe_container(container_name):
    print(f"\n🚨 Wiping container: {container_name}")
    container_client = blob_service_client.get_container_client(container_name)

    try:
        blob_list = list(container_client.list_blobs())
        if not blob_list:
            print("✅ No files to delete.")
            return

        for blob in blob_list:
            container_client.delete_blob(blob.name)
            print(f"🗑️ Deleted: {blob.name}")
        print(f"✅ Container '{container_name}' wiped successfully.")

    except Exception as e:
        print(f"❌ Failed to wipe container '{container_name}': {e}")

if __name__ == "__main__":
    wipe_container(RAW_CONTAINER)
    wipe_container(TRANSFORMED_CONTAINER)
