{{
    config(materialized='view')
}}

select
    order_date,
    count(distinct order_id)     as order_count,
    count(distinct customer_id)  as unique_customers,
    count(distinct product_id)   as unique_products,
    sum(quantity)                as total_quantity
from {{ ref('orders_incremental') }}
group by order_date
order by order_date
