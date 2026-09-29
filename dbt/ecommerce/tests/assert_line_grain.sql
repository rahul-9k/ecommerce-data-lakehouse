select order_id, line_number
from {{ ref('fact_order_items') }}
group by order_id, line_number
having count(*) <> 1
