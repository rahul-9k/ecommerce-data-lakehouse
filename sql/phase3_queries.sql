-- Run after Phase 3 completes. Example queries, no destructive statements.
SELECT 'products' AS table_name, count(*) AS rows FROM landing.products
UNION ALL SELECT 'customers', count(*) FROM landing.customers
UNION ALL SELECT 'orders', count(*) FROM landing.orders
UNION ALL SELECT 'order_items', count(*) FROM landing.order_items;

SELECT count(*) AS order_items, count(DISTINCT order_id) AS orders,
       sum(line_amount) AS gross_order_value_usd_all_statuses
FROM analytics.fact_order_items;

SELECT order_day, order_count, order_item_count,
       gross_order_value_usd_all_statuses, delivered_order_value_usd
FROM analytics.mart_daily_sales ORDER BY order_day LIMIT 10;

SELECT p.category, sum(f.line_amount) AS delivered_order_value_usd
FROM analytics.fact_order_items f
JOIN analytics.dim_product p USING (product_key)
WHERE f.order_status = 'delivered'
GROUP BY p.category ORDER BY delivered_order_value_usd DESC;

-- Do NOT sum per-category distinct order counts to get total distinct orders:
-- one order can include several categories.
