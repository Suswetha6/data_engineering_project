CREATE OR REPLACE FUNCTION get_customer_sales(cust_id INT)
RETURNS TABLE (
    order_id INT,
    order_date DATE,
    total_sales NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.order_id,
        o.order_date,
        SUM(od.sales)
    FROM orders o
    JOIN order_detail od USING (order_id)
    WHERE o.customer_id = cust_id
    GROUP BY o.order_id, o.order_date;
END;
$$ LANGUAGE plpgsql;
