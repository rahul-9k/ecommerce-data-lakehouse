with expected as (
    select i.order_item_id, i.order_id, i.line_number,
           md5('customer:' || i.customer_id) as customer_key,
           md5('product:' || i.product_id::text) as product_key,
           to_char(i.order_day, 'YYYYMMDD')::integer as date_key,
           i.order_date, i.order_status, i.currency, i.quantity, i.unit_price,
           i.line_amount, i.updated_at, i.source_batch_id, i.ingested_at, i.source_system
    from {{ ref('stg_order_items') }} i
), actual as (
    select order_item_id, order_id, line_number, customer_key, product_key, date_key,
           order_date, order_status, currency, quantity, unit_price, line_amount,
           updated_at, source_batch_id, ingested_at, source_system
    from {{ ref('fact_order_items') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
