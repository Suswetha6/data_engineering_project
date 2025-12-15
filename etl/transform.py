import pandas as pd
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

def extract_from_gsheets(sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(
        "service_account.json", scopes=scopes
    )
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    data = worksheet.get_all_records()
    return pd.DataFrame(data)


def normalize(df: pd.DataFrame):
  # ---------- CUSTOMER ----------
  customers = (
    df[["Customer Name", "Customer Segment", "Province", "Region"]]
    .drop_duplicates()
    .reset_index(drop=True)
  )
  customers = customers.assign(customer_id=customers.index + 1).rename(columns={
    "Customer Name": "customer_name",
    "Customer Segment": "customer_segment",
    "Province": "province",
    "Region": "region"
  })[["customer_id", "customer_name", "customer_segment", "province", "region"]]

  # mapping from original name -> numeric id
  cust_map = dict(zip(customers["customer_name"], customers["customer_id"]))

  # ---------- PRODUCTS ----------
  products = (
    df[["Product Name", "Product Category", "Product Sub-Category",
      "Product Container", "Unit Price", "Product Base Margin"]]
    .drop_duplicates()
    .reset_index(drop=True)
  )
  products = products.assign(product_id=products.index + 1).rename(columns={
    "Product Name": "product_name",
    "Product Category": "product_category",
    "Product Sub-Category": "product_sub_category",
    "Product Container": "product_container",
    "Unit Price": "unit_price",
    "Product Base Margin": "product_base_margin"
  })[["product_id", "product_name", "product_category", "product_sub_category",
    "product_container", "unit_price", "product_base_margin"]]

  prod_map = dict(zip(products["product_name"], products["product_id"]))

  # ---------- ORDERS ----------
  orders = (
    df[["Order ID", "Order Date", "Ship Date",
      "Order Priority", "Shipping Cost", "Customer Name"]]
    .drop_duplicates(subset=["Order ID"])
    .reset_index(drop=True)
  )
  orders = orders.assign(order_id=orders.index + 1)
  # map customer name to numeric id
  orders = orders.assign(customer_id=orders["Customer Name"].map(cust_map))
  orders = orders.rename(columns={
    "Order ID": "orig_order_id",
    "Order Date": "order_date",
    "Ship Date": "ship_date",
    "Order Priority": "order_priority",
    "Shipping Cost": "shipping_cost"
  })[["order_id", "orig_order_id", "order_date", "ship_date", "order_priority", "shipping_cost", "customer_id"]]

  order_map = dict(zip(orders["orig_order_id"], orders["order_id"]))

  # ---------- ORDER DETAILS ----------
  order_detail = (
    df.assign(
      order_id=lambda x: x["Order ID"].map(order_map),
      product_id=lambda x: x["Product Name"].map(prod_map),
      customer_id=lambda x: x["Customer Name"].map(cust_map)
    )
    [["order_id", "product_id", "Order Quantity",
      "Discount", "Sales", "Profit"]]
    .rename(columns={
      "Order Quantity": "order_quantity",
      "Discount": "discount",
      "Sales": "sales",
      "Profit": "profit"
    })
  )

  return customers, orders, products, order_detail
