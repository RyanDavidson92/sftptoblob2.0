import os
import paramiko
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read SFTP credentials from environment
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")

def list_sftp_files():
    try:
        # Create the transport and connect
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

        # Open the SFTP session
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("✅ Connected to SFTP server.")

        # List files in the current directory
        remote_dir = "uploads"   #### THIS IS WHERE THE SCRIPT IS LOOKING FOR FILES IN THE VPS
        files = sftp.listdir(remote_dir)

        if files:
            print("📁 Files in remote directory:")
            for f in files:
                print(f" - {f}")
        else:
            print("📁 Remote directory is empty.")

        # Close the connection
        sftp.close()
        transport.close()
        print("🔌 Connection closed.")

    except Exception as e:
        print("❌ Failed to connect or list files.")
        print("Error:", e)

if __name__ == "__main__":
    list_sftp_files()
