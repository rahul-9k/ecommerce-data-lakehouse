select order_id, customer_id, order_date,
       (order_date at time zone 'UTC')::date as order_day,
       status as order_status, currency, order_total, updated_at,
       _batch_id as source_batch_id, _ingested_at as ingested_at, _source as source_system
from {{ source('curated', 'orders') }}
