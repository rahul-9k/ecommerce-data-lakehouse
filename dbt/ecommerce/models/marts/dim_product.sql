-- Keep catalogue price as an attribute. Never use it to reprice old line items.
select md5('product:' || product_id::text) as product_key,
       product_id, product_name, category, catalog_price,
       source_batch_id, ingested_at, source_system
from {{ ref('stg_products') }}
