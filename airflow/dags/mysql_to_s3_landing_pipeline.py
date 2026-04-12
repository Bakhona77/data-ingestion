from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime
from urllib.parse import quote_plus
import pandas as pd
import boto3
from sqlalchemy import create_engine
from io import BytesIO


# -----------------------------
# CONFIG FROM AIRFLOW VARIABLES
# -----------------------------
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")
S3_BUCKET = Variable.get("S3_BUCKET")

user = Variable.get("MYSQL_USER")
password = quote_plus(Variable.get("MYSQL_PASSWORD"))
host = Variable.get("MYSQL_HOST")
port = Variable.get("MYSQL_PORT")
db = Variable.get("MYSQL_DATABASE")


# -----------------------------
# TABLE LIST
# -----------------------------
TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_translation",
    "geolocation"
]

# -----------------------------
# AWS CLIENT
# -----------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# -----------------------------
# MYSQL ENGINE
# -----------------------------
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")


# -----------------------------
# LANDING FUNCTION
# -----------------------------
def land_to_s3(table_name):

    query = f"SELECT * FROM {table_name}"

    conn = engine.raw_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    if df.empty:
        print(f"[SKIP] {table_name} is empty")
        return

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    date_path = datetime.today().strftime("%Y-%m-%d")

    s3_key = f"landing/{table_name}/{date_path}/{table_name}.parquet"

    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)

    print(f"[SUCCESS] Loaded {table_name} → {s3_key}")


# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="mysql_to_s3_landing_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "mysql", "s3"]
) as dag:

    # -----------------------------
    # LANDING TASK GROUP
    # -----------------------------
    with TaskGroup(group_id="landing_batch") as landing_batch:

        for table in TABLES:
            PythonOperator(
                task_id=f"{table}_land",
                python_callable=land_to_s3,
                op_kwargs={"table_name": table}
            )

