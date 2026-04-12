import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv


load_dotenv("config/.env")

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")
password = quote_plus(password)
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

DATA_PATH = "data/raw"

files = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation"
}

for file, table in files.items():
    path = os.path.join(DATA_PATH, file)

    print(f"Loading {path} → {table}")

    df = pd.read_csv(path)

    df.to_sql(table, con=engine, if_exists="replace", index=False)

    print(f"Loaded {table}")

print("All data loaded successfully")