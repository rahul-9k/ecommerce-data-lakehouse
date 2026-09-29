-- Independent order count/value from order headers prevents line-join double-counting.
with headers as (
    select order_day, currency, count(*)::bigint as order_count,
           sum(order_total)::numeric(28,2) as gross_order_value_usd_all_statuses,
           sum(case when order_status='delivered' then order_total else 0 end)::numeric(28,2) as delivered_order_value_usd,
           sum(case when order_status='cancelled' then order_total else 0 end)::numeric(28,2) as cancelled_order_value_usd
    from {{ ref('stg_orders') }} group by order_day, currency
), lines as (
    select order_day, currency, count(*)::bigint as order_item_count, sum(quantity)::bigint as units_ordered
    from {{ ref('stg_order_items') }} group by order_day, currency
), expected as (
    select h.order_day, h.currency, h.order_count, l.order_item_count, l.units_ordered,
           h.gross_order_value_usd_all_statuses, h.delivered_order_value_usd, h.cancelled_order_value_usd
    from headers h join lines l using (order_day, currency)
), actual as (
    select order_day, currency, order_count, order_item_count, units_ordered,
           gross_order_value_usd_all_statuses, delivered_order_value_usd, cancelled_order_value_usd
    from {{ ref('mart_daily_sales') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
