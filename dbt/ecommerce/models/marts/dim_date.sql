with bounds as (
    select min(order_day)::timestamp as start_day, max(order_day)::timestamp as end_day
    from {{ ref('stg_orders') }}
), dates as (
    select generate_series(start_day, end_day, interval '1 day')::date as full_date
    from bounds
)
select to_char(full_date, 'YYYYMMDD')::integer as date_key,
       full_date, extract(year from full_date)::integer as year_number,
       extract(month from full_date)::integer as month_number,
       extract(day from full_date)::integer as day_number,
       extract(isodow from full_date)::integer as weekday_number,
       extract(isodow from full_date) in (6, 7) as is_weekend
from dates
