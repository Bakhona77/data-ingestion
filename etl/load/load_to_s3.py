import os, sys
import pandas as pd
import boto3
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO

load_dotenv("config/.env")

# -----------------------
# MySQL Connection
# -----------------------
user = os.getenv("MYSQL_USER")
password = quote_plus(os.getenv("MYSQL_PASSWORD"))
host = os.getenv("MYSQL_HOST")
port = os.getenv("MYSQL_PORT")
db = os.getenv("MYSQL_DATABASE")

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
)

try:
    with engine.connect() as conn:
        print("Database connection successful")
except Exception as e:
    print("Database connection failed:", e)
    sys.exit(1)


def get_dataframe(tablename):
    try:
        query = f"SELECT * FROM {tablename}"
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"ERROR: Failed to read table '{tablename}'")
        print(f"Reason: {e}")
        sys.exit(1)

# -----------------------
# CLI Args
# -----------------------
args = sys.argv[1:]

if len(args) < 1:
    raise ValueError("Usage: python script.py <tablename>")

tablename = args[0]


# -----------------------
# AWS S3 Client
# -----------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket = os.getenv("S3_LANDING_BUCKET")

# -----------------------
# Extract + Transform
# -----------------------
df = get_dataframe(tablename)

buffer = BytesIO()
df.to_parquet(buffer, index=False, compression="snappy")
buffer.seek(0)

today = datetime.today().strftime("%Y-%m-%d")

s3_key = f"raw/{tablename}/{today}/{tablename}.parquet"

# -----------------------
# Load
# -----------------------
s3.upload_fileobj(buffer, bucket, s3_key)

print(f"Uploaded to s3://{bucket}/{s3_key}")