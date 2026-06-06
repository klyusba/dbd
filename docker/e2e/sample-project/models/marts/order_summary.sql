{{
    config(materialized='view')
}}

select
    count(*)                     as total_customers,
    count(distinct customer_id)  as unique_customers
from {{ ref('customers_incremental') }}
