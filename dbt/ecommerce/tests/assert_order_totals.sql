with amounts as (
    select order_id, sum(line_amount) as total
    from {{ ref('fact_order_items') }}
    group by order_id
)
select coalesce(o.order_id, a.order_id) as order_id
from {{ ref('stg_orders') }} o
full outer join amounts a on o.order_id = a.order_id
where o.order_id is null or a.order_id is null or o.order_total is distinct from a.total
