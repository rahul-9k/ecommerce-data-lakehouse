with bounds as (
    select min(order_day)::timestamp as first_day, max(order_day)::timestamp as last_day
    from {{ ref('stg_orders') }}
), calendar as (
    select generate_series(first_day, last_day, interval '1 day')::date as full_date from bounds
), expected as (
    select to_char(full_date, 'YYYYMMDD')::integer as date_key, full_date,
           extract(year from full_date)::integer as year_number,
           extract(month from full_date)::integer as month_number,
           extract(day from full_date)::integer as day_number,
           extract(isodow from full_date)::integer as weekday_number,
           extract(isodow from full_date) in (6,7) as is_weekend
    from calendar
), actual as (
    select date_key, full_date, year_number, month_number, day_number, weekday_number, is_weekend
    from {{ ref('dim_date') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
