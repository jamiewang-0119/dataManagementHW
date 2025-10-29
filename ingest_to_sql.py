import os
import pandas as pd
from sqlalchemy import create_engine, text

# Read connection information from environment variables
SERVER   = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DB")
USERNAME = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASSWORD")

DRIVER = "ODBC Driver 18 for SQL Server"

# Build connection string for Azure SQL Database
conn_str = (
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}:1433/"
    f"{DATABASE}?driver={DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"
)

engine = create_engine(conn_str, fast_executemany=True)

# File paths for the Canvas CSV data
brand_path = "data/brand-detail-url-etc_0_0_0.csv"
spend_path = "data/2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv"

# Chunk size for reading and inserting data
CHUNK = 100000

def load_csv_to_sql(path, table_name):
    print(f"Loading {path} into {table_name} ...")
    first = True
    for chunk in pd.read_csv(path, chunksize=CHUNK):
        if first:
            chunk.to_sql(table_name, engine, index=False, if_exists="replace")
            first = False
        else:
            chunk.to_sql(table_name, engine, index=False, if_exists="append")
    print(f"Completed: {table_name}")

# Load both datasets
load_csv_to_sql(brand_path, "BrandDetail")
load_csv_to_sql(spend_path, "DailySpend")

# Verify data import by showing first 5 rows from each table
with engine.begin() as conn:
    for t in ["BrandDetail", "DailySpend"]:
        print(f"\nSample from {t}:")
        rows = conn.execute(text(f"SELECT TOP (5) * FROM {t}")).fetchall()
        for r in rows:
            print(dict(r._mapping))

print("\nCSV data successfully uploaded to Azure SQL Database.")
