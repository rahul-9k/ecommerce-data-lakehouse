-- Deterministic surrogate ID; not SCD Type 2 and not a cryptographic security key.
select md5('customer:' || customer_id) as customer_key,
       customer_id, customer_name, city, country, created_at, updated_at,
       source_batch_id, ingested_at, source_system
from {{ ref('stg_customers') }}
