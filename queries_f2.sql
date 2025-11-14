-- Query 1: Client, transaction, product, and segment details
SELECT
    c.client_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    tl.transaction_id,
    tl.transaction_date,
    tl.payment_method,
    tl.status,
    tl.transaction_total,
    p.product_id,
    p.name AS product_name,
    p.segment_id,
    st.segment_name,
    td.quantity,
    td.unit_price,
    td.line_total
FROM transaction_details_f2 AS td
JOIN transaction_log_f2 AS tl ON td.transaction_id = tl.transaction_id
JOIN client_directory_f2 AS c ON tl.client_id = c.client_id
JOIN product_master_f2 AS p ON td.product_id = p.product_id
JOIN segment_taxonomy_f2 AS st ON p.segment_id = st.segment_id
ORDER BY tl.transaction_date, td.detail_id;

-- Query 2: Revenue per client for completed transactions
SELECT
    c.client_id,
    c.first_name,
    c.last_name,
    c.email,
    SUM(tl.transaction_total) AS total_revenue
FROM transaction_log_f2 AS tl
JOIN client_directory_f2 AS c ON tl.client_id = c.client_id
WHERE tl.status = 'completed'
GROUP BY c.client_id, c.first_name, c.last_name, c.email
ORDER BY total_revenue DESC;
