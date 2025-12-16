CREATE OR REPLACE VIEW order_summary AS
SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    c.region,
    SUM(od.sales)  AS total_sales,
    SUM(od.profit) AS total_profit
FROM orders o
JOIN customer c USING (customer_id)
JOIN order_detail od USING (order_id)
GROUP BY o.order_id, o.order_date, c.customer_name, c.region;


CREATE OR REPLACE VIEW product_performance AS
SELECT
    p.product_name,
    p.product_category,
    SUM(od.order_quantity) AS units_sold,
    SUM(od.sales) AS revenue,
    SUM(od.profit) AS profit
FROM product p
JOIN order_detail od USING (product_id)
GROUP BY p.product_name, p.product_category;
