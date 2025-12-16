-- -- Total revenue & profit
SELECT
    SUM(sales)  AS total_sales,
    SUM(profit) AS total_profit,
    AVG(discount) AS avg_discount
FROM order_detail;

-- -- Orders per customer
SELECT
    c.customer_name,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM customer c
JOIN orders o USING (customer_id)
GROUP BY c.customer_name
ORDER BY total_orders DESC;

-- -- Order-level report
SELECT
    o.order_id,
    c.customer_name,
    p.product_name,
    od.order_quantity,
    od.sales,
    od.profit
FROM order_detail od
JOIN orders o USING (order_id)
JOIN customer c USING (customer_id)
JOIN product p USING (product_id)
LIMIT 20;



-- -- Duplicate customers (same name + region)
SELECT customer_name, region, COUNT(*)
FROM customer
GROUP BY customer_name, region
HAVING COUNT(*) > 1;

-- -- Invalid values
SELECT *
FROM order_detail
WHERE order_quantity <= 0
   OR sales < 0;


-- -- Customers per region
SELECT
    region,
    COUNT(*) AS customer_count
FROM customer
GROUP BY region
ORDER BY customer_count DESC;

-- -- Average profit per product category
SELECT
    p.product_category,
    AVG(od.profit) AS avg_profit
FROM order_detail od
JOIN product p USING (product_id)
GROUP BY p.product_category;
