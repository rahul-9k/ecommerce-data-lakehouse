select customer_id, customer_name, city, country, created_at, updated_at,
       _batch_id as source_batch_id, _ingested_at as ingested_at, _source as source_system
from {{ source('curated', 'customers') }}
