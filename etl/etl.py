import psycopg2
from extract import extract_from_gsheets
from transform import normalize
from validate import validate_order_detail
from load import load_table
from logger import setup_logger
from dotenv import load_dotenv
import os

load_dotenv()

setup_logger()

df = extract_from_gsheets(
    sheet_id="1CCZ8ZQ8kyO4pmBxKeN2byyA8jEL3_vW5lF6CjitC7x0",
    worksheet_name="superstoreSales.csv"
)

customers, orders, products, order_detail = normalize(df)

valid_od, rejected_od = validate_order_detail(order_detail)
rejected_od.to_csv("etl/logs/rejected_rows.csv", index=False)

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

def clean_nulls(df):
    return df.replace("", None).where(df.notna(), None)

customers = clean_nulls(customers)
orders = clean_nulls(orders)
products = clean_nulls(products)
valid_od = clean_nulls(valid_od)



load_table(conn, "customer",
           ["customer_id", "customer_name", "customer_segment", "province", "region"],
           customers)

load_table(
    conn,
    "orders",
    ["order_id", "order_date", "ship_date",
     "order_priority", "shipping_cost", "customer_id"],
    orders
)


load_table(conn, "product",
           ["product_id", "product_name", "product_category",
            "product_sub_category", "product_container",
            "unit_price", "product_base_margin"],
           products)

load_table(conn, "order_detail",
           ["order_id", "product_id", "order_quantity",
            "discount", "sales", "profit"],
           valid_od)
