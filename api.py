from fastapi import FastAPI, HTTPException
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import numpy as np
from etl.transform import normalize
from etl.validate import validate_order_detail
from etl.load import load_table

load_dotenv()

app = FastAPI(title="Superstore Ingestion API")

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def clean_nulls(df):
    return (
        df
        .replace({np.nan: None, "NaN": None, "": None})
    )

REQUIRED_FIELDS = [
    "Order ID",
    "Order Date",
    "Customer Name",
    "Product Name",
    "Order Quantity",
    "Sales"
]

@app.post("/ingest/order")
def ingest_order(payload: dict):
    # 1️⃣ Validate required fields
    missing = [f for f in REQUIRED_FIELDS if not payload.get(f)]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required fields: {missing}"
        )

    # 2️⃣ Convert payload → DataFrame
    df = pd.DataFrame([payload])

    # 3️⃣ Normalize (reuse ETL logic)
    customers, orders, products, order_detail = normalize(df)

    # 4️⃣ Validate order_detail
    valid_od, rejected_od = validate_order_detail(order_detail)

    if valid_od.empty:
        return {
            "status": "rejected",
            "reason": "Validation failed",
            "rows": rejected_od.to_dict(orient="records")
        }

    # 5️⃣ Clean nulls
    customers = clean_nulls(customers)
    orders = clean_nulls(orders)
    products = clean_nulls(products)
    valid_od = clean_nulls(valid_od)

    # 6️⃣ Load into DB (transaction-safe)
    conn = get_conn()
    try:
        load_table(
            conn, "customer",
            ["customer_id", "customer_name", "customer_segment", "province", "region"],
            customers
        )

        load_table(
            conn, "orders",
            ["order_id", "order_date", "ship_date",
             "order_priority", "shipping_cost", "customer_id"],
            orders
        )

        load_table(
            conn, "product",
            ["product_id", "product_name", "product_category",
             "product_sub_category", "product_container",
             "unit_price", "product_base_margin"],
            products
        )

        load_table(
            conn, "order_detail",
            ["order_id", "product_id", "order_quantity",
             "discount", "sales", "profit"],
            valid_od
        )

    finally:
        conn.close()

    return {
        "status": "success",
        "order_id": payload["Order ID"]
    }
