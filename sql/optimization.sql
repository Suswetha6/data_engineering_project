CREATE INDEX idx_orders_customer_id
ON orders(customer_id);

CREATE INDEX idx_order_detail_order_id
ON order_detail(order_id);

CREATE INDEX idx_order_detail_product_id
ON order_detail(product_id);

CREATE INDEX idx_customer_region


EXPLAIN ANALYZE
SELECT
    c.region,
    SUM(od.sales)
FROM order_detail od
JOIN orders o USING (order_id)
JOIN customer c USING (customer_id)
GROUP BY c.region;
