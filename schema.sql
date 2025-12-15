CREATE TABLE customer (
  customer_id TEXT PRIMARY KEY,
  customer_name TEXT NOT NULL,
  customer_segment TEXT,
  province TEXT,
  region TEXT
);

CREATE TABLE orders (
  order_id TEXT PRIMARY KEY,
  order_date DATE,
  ship_date DATE,
  order_priority TEXT,
  shipping_cost NUMERIC CHECK (shipping_cost >= 0),
  customer_id TEXT REFERENCES customer(customer_id)
);

CREATE TABLE product (
  product_id TEXT PRIMARY KEY,
  product_category TEXT,
  product_sub_category TEXT,
  product_container TEXT,
  unit_price NUMERIC CHECK (unit_price >= 0),
  product_base_margin NUMERIC
);

CREATE TABLE order_detail (
  order_detail_id SERIAL PRIMARY KEY,
  order_id TEXT REFERENCES orders(order_id),
  product_id TEXT REFERENCES product(product_id),
  order_quantity INT CHECK (order_quantity > 0),
  discount NUMERIC CHECK (discount BETWEEN 0 AND 1),
  sales NUMERIC,
  profit NUMERIC,
  UNIQUE (order_id, product_id)
);
