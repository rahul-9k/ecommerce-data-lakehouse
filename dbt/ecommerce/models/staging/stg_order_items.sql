select order_item_id, order_id, line_number, product_id, customer_id,
       order_date, (order_date at time zone 'UTC')::date as order_day,
       order_status, currency, quantity, unit_price, line_amount, updated_at,
       _batch_id as source_batch_id, _ingested_at as ingested_at, _source as source_system
from {{ source('curated', 'order_items') }}
