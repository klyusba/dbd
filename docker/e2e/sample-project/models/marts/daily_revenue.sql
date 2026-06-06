{{
    config(materialized='view')
}}

select
    order_date,
    count(distinct order_id)     as order_count,
    count(distinct customer_id)  as unique_customers,
    count(distinct product_id)   as unique_products,
    sum(quantity)                as total_quantity,
    sum(line_total)              as daily_revenue,
    avg(line_total)              as avg_order_value
from {{ ref('int_orders_enriched') }}
group by order_date
order by order_date
