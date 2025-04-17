import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from io import BytesIO

load_dotenv()

# ENV CONFIG
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY")
TRANSFORMED_CONTAINER = os.getenv("AZURE_TRANSFORMED_CONTAINER")

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

TABLE_NAME = "TestDB.dbo.usps_ebill_prod"

# CONNECT TO BLOB SERVICE
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=ACCOUNT_KEY
)
container_client = blob_service_client.get_container_client(TRANSFORMED_CONTAINER)

# SQL CONNECTION SETUP
conn_str = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD}"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.fast_executemany = True

# Insert SQL (29 fields, omit ID and CreatedDate)
insert_sql = f"""
INSERT INTO {TABLE_NAME} (
    ControlNo, ChildID, TrackingNumber, InvoiceNumber, InvoiceDate, ShipDate,
    Length, Height, Width, DimUOM, ServiceLevel, ShipperNumber,
    OriginZip, DestinationZip, Zone, BilledWeight_LB, WeightUnit,
    PackageCharge, FuelSurcharge, ResidentialSurcharge, DASCharge, TotalCharge,
    AccessorialCode, AccessorialDescription, PackageStatus, ReceiverName,
    ReceiverCity, ReceiverState, ReceiverCountry
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Expected columns (lowercase)
expected_cols = [
    "controlno", "childid", "trackingnumber", "invoicenumber", "invoicedate", "shipdate",
    "length", "height", "width", "dimuom", "servicelevel", "shippernumber",
    "originzip", "destinationzip", "zone", "billedweight_lb", "weightunit",
    "packagecharge", "fuelsurcharge", "residentialsurcharge", "dascharge", "totalcharge",
    "accessorialcode", "accessorialdescription", "packagestatus", "receivername",
    "receivercity", "receiverstate", "receivercountry"
]

# PROCESS EACH FILE
for blob in container_client.list_blobs():
    if not blob.name.endswith(".csv"):
        continue

    print(f"📥 Processing file: {blob.name}")
    blob_client = container_client.get_blob_client(blob.name)

    try:
        # ✅ MISSING BLOCK - insert this to fix the error
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(BytesIO(blob_data))

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower()
        df = df.rename(columns={"clientid": "childid"})
        df = df.drop(columns=["createddate"], errors="ignore")

        # Check for missing columns
        if not all(col in df.columns for col in expected_cols):
            missing = [col for col in expected_cols if col not in df.columns]
            raise ValueError(f"Missing columns: {missing}")

        # Reorder columns
        df = df[expected_cols]

        # ✅ Wrap the insert logic
        try:
            cursor.executemany(insert_sql, df.values.tolist())
            conn.commit()
            print(f"✅ Inserted {len(df)} rows from {blob.name}")

        except pyodbc.IntegrityError as e:
            if "UQ_Tracking_Control" in str(e):
                print(f"⚠️  Skipped {blob.name}: duplicate TrackingNumber + ControlNo already in table.")
            else:
                print(f"❌ IntegrityError in {blob.name}: {e}")

    except Exception as e:
        print(f"❌ Unexpected error in {blob.name}: {e}")



cursor.close()
conn.close()
print("🏁 All files processed.")
