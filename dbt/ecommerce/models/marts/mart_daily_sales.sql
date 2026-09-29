-- All statuses retained. Gross value is NOT recognized/accounting revenue.
select d.full_date as order_day, f.currency,
       count(distinct f.order_id)::bigint as order_count,
       count(*)::bigint as order_item_count,
       sum(f.quantity)::bigint as units_ordered,
       sum(f.line_amount)::numeric(28,2) as gross_order_value_usd_all_statuses,
       sum(case when f.order_status = 'delivered' then f.line_amount else 0 end)::numeric(28,2)
           as delivered_order_value_usd,
       sum(case when f.order_status = 'cancelled' then f.line_amount else 0 end)::numeric(28,2)
           as cancelled_order_value_usd
from {{ ref('fact_order_items') }} f
join {{ ref('dim_date') }} d on f.date_key = d.date_key
group by d.full_date, f.currency
