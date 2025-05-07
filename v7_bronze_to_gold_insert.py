

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

# TABLE NAMES
USPS_TABLE = "TestDB.dbo.usps_ebill_prod"
UPS_TABLE = "TestDB.dbo.ups_ebill_prod"
Control_master = "TestDB.dbo.Control_master"

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

# USPS expected columns (lowercase)
usps_cols = [
    "controlno", "childid", "trackingnumber", "invoicenumber", "invoicedate", "shipdate",
    "length", "height", "width", "dimuom", "servicelevel", "shippernumber",
    "originzip", "destinationzip", "zone", "billedweight_lb", "weightunit",
    "packagecharge", "fuelsurcharge", "residentialsurcharge", "dascharge", "totalcharge",
    "accessorialcode", "accessorialdescription", "packagestatus", "receivername",
    "receivercity", "receiverstate", "receivercountry"
]

# UPS expected columns (match ups_ebill_prod)
ups_cols = [
    "Lead Shipment Number", "ControlNo", "ChildID", "BillToAccountNo", "InvoiceDt", "Bill Option Code",
    "Container Type", "Transaction Date", "Package Quantity", "Sender Country", "Receiver Country",
    "Charge Category Code", "Charge Classification Code", "Charge Category Detail Code",
    "Charge Description", "Zone", "Billed Weight", "Billed Weight Unit of Measure",
    "Billed Weight Type", "Net Amount", "Incentive Amount", "Tracking Number",
    "Sender State", "Receiver State", "Invoice Currency Code"
]

# USPS insert SQL
insert_usps_sql = f"""
    INSERT INTO {USPS_TABLE} (
        ControlNo, ChildID, TrackingNumber, InvoiceNumber, InvoiceDate, ShipDate,
        Length, Height, Width, DimUOM, ServiceLevel, ShipperNumber,
        OriginZip, DestinationZip, Zone, BilledWeight_LB, WeightUnit,
        PackageCharge, FuelSurcharge, ResidentialSurcharge, DASCharge, TotalCharge,
        AccessorialCode, AccessorialDescription, PackageStatus, ReceiverName,
        ReceiverCity, ReceiverState, ReceiverCountry
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# UPS insert SQL
insert_ups_sql = f"""
    INSERT INTO {UPS_TABLE} (
        [Lead Shipment Number], ControlNo, ChildID, BillToAccountNo, InvoiceDt, [Bill Option Code],
        [Container Type], [Transaction Date], [Package Quantity], [Sender Country], [Receiver Country],
        [Charge Category Code], [Charge Classification Code], [Charge Category Detail Code],
        [Charge Description], Zone, [Billed Weight], [Billed Weight Unit of Measure],
        [Billed Weight Type], [Net Amount], [Incentive Amount], [Tracking Number],
        [Sender State], [Receiver State], [Invoice Currency Code]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def process_usps_blob(df, blob_name):
    df.columns = df.columns.str.lower()
    df = df.drop(columns=["createddate"], errors="ignore")

    if not all(col in df.columns for col in usps_cols):
        missing = [col for col in usps_cols if col not in df.columns]
        raise ValueError(f"Missing USPS columns: {missing}")

    df = df[usps_cols]
    cursor.executemany(insert_usps_sql, df.values.tolist())
    conn.commit()
    print(f"✅ Inserted {len(df)} rows into usps_ebill_prod from {blob_name}")

def process_ups_blob(df, blob_name):
    if not all(col in df.columns for col in ups_cols):
        missing = [col for col in ups_cols if col not in df.columns]
        raise ValueError(f"Missing UPS columns: {missing}")

    df = df[ups_cols]
    cursor.executemany(insert_ups_sql, df.values.tolist())
    conn.commit()
    print(f"✅ Inserted {len(df)} rows into ups_ebill_prod from {blob_name}")

def process_transformed_blob(blob):
    if not blob.name.endswith(".csv"):
        return

    # Check if already processed
    cursor.execute(f"SELECT 1 FROM {Control_master} WHERE FileName = ?", (blob.name,))
    if cursor.fetchone():
        print(f"⚠️ Skipped {blob.name}: already processed.")
        return

    print(f"\n📥 Processing file: {blob.name}")
    blob_client = container_client.get_blob_client(blob.name)

    try:
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(BytesIO(blob_data))

        df.columns = df.columns.str.strip()
        df = df.rename(columns={"clientid": "ChildID", "controlno": "ControlNo"})
        carrier = df.get("carrier", [None])[0]

        if carrier == "USPS":
            process_usps_blob(df, blob.name)
        elif carrier == "UPS":
            process_ups_blob(df, blob.name)
        else:
            print(f"⚠️ Skipped {blob.name}: unknown carrier type '{carrier}'")
            return


    except pyodbc.IntegrityError as e:
        error_msg = str(e)
        if "duplicate" in error_msg.lower() or "unique" in error_msg.lower():
            print(f"⚠️ Skipped {blob.name}: Duplicate record.")
        else:
            print(f"❌ Integrity error in {blob.name}: {error_msg.splitlines()[0]}")

    except Exception as e:
        print(f"❌ General error in {blob.name}: {str(e).splitlines()[0]}")


# MAIN EXECUTION
for blob in container_client.list_blobs():
    process_transformed_blob(blob)

cursor.close()
conn.close()
print("\n🏁 All files processed.")