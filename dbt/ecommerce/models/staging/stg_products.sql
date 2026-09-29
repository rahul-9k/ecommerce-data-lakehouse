select product_id, product_name, category, catalog_price,
       _batch_id as source_batch_id, _ingested_at as ingested_at, _source as source_system
from {{ source('curated', 'products') }}
