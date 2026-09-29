with expected as (
    select md5('product:' || product_id::text) as product_key, product_id, product_name, category, catalog_price, source_batch_id, ingested_at, source_system
    from {{ ref('stg_products') }}
), actual as (
    select product_key, product_id, product_name, category, catalog_price, source_batch_id, ingested_at, source_system from {{ ref('dim_product') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
