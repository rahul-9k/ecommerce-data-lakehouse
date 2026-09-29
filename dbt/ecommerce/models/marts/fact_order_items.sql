-- Grain: exactly ONE row per order_item_id, not one row per order.
select i.order_item_id, i.order_id, i.line_number,
       c.customer_key, p.product_key, d.date_key,
       i.order_date, i.order_status, i.currency,
       i.quantity, i.unit_price, i.line_amount, i.updated_at,
       i.source_batch_id, i.ingested_at, i.source_system
from {{ ref('stg_order_items') }} i
join {{ ref('dim_customer') }} c on i.customer_id = c.customer_id
join {{ ref('dim_product') }} p on i.product_id = p.product_id
join {{ ref('dim_date') }} d on i.order_day = d.full_date
