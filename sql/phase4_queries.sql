-- Read-only learning queries. The Phase 3 baseline is intentionally retained.
SELECT 'phase3_original' AS dataset, count(*) AS orders FROM landing.orders
UNION ALL
SELECT 'phase4_current', count(*) FROM landing_inc.orders;

SELECT last_change_seq, models_for_seq, status
FROM pipeline_inc.state WHERE slot=1;

SELECT batch_id, first_seq, last_seq, stats, applied_at
FROM pipeline_inc.batches ORDER BY first_seq;

SELECT change_seq, event_id, order_id, source_updated_at
FROM pipeline_inc.events ORDER BY change_seq LIMIT 10;

SELECT order_id, status, order_total, updated_at, _batch_id
FROM landing_inc.orders WHERE _batch_id='delta_001'
ORDER BY order_id LIMIT 10;

SELECT count(*) AS fact_rows, count(distinct order_id) AS distinct_orders,
       sum(line_amount) AS synthetic_gross_order_value_all_statuses
FROM analytics_inc.fact_order_items;

-- Correct result: zero rows; counts and amounts should reconcile at ORDER grain.
SELECT o.order_id
FROM landing_inc.orders o
LEFT JOIN (
    SELECT order_id,sum(line_amount) AS line_total
    FROM analytics_inc.fact_order_items GROUP BY order_id
) f USING (order_id)
WHERE o.order_total IS DISTINCT FROM f.line_total;
