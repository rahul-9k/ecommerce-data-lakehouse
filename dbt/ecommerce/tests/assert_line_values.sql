select order_item_id
from {{ ref('fact_order_items') }}
where quantity <= 0 or unit_price < 0
   or line_amount is distinct from (quantity * unit_price)::numeric(28,2)
