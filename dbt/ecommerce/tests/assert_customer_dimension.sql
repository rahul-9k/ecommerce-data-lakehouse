with expected as (
    select md5('customer:' || customer_id) as customer_key, customer_id, customer_name, city, country, created_at, updated_at, source_batch_id, ingested_at, source_system
    from {{ ref('stg_customers') }}
), actual as (
    select customer_key, customer_id, customer_name, city, country, created_at, updated_at, source_batch_id, ingested_at, source_system from {{ ref('dim_customer') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
