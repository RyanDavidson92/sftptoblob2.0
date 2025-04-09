# SFTP to Azure SQL Pipeline2.0

This project is a Python-based data ingestion pipeline that:
- Downloads raw invoice files from a DigitalOcean SFTP server (Much cheaper than Azure's SFTP option)
- Uploads them to Azure Blob Storage (`clientinvoicesraw`)
- Maps raw columns to a standard SQL format using a pre-defined dictionary
- Inserts the data into Azure SQL (`control_master` and `usps_ebill_prod`)
- Triggers stored procedures for auditing

## Technologies
- Python 3.10
- Paramiko (SFTP)
- Azure Blob Storage SDK
- pyodbc / pymssql for SQL Server connection

## Purpose
This pipeline supports the backend for a low-cost, automated data audit system.

## Security
No credentials are included in this repo. All secrets are managed through environment variables or `.env` files (excluded from Git).

## Author
Ryan Davidson
