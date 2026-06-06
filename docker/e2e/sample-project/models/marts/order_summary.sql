{{
    config(materialized='view')
}}

select
    count(*)                     as total_orders,
    count(distinct customer_id)  as unique_customers,
    count(distinct product_id)   as unique_products,
    sum(quantity)                as total_quantity,
    min(order_date)              as first_order_date,
    max(order_date)              as last_order_date
from {{ ref('stg_orders') }}
