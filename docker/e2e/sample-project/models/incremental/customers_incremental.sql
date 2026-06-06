{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

select
    customer_id,
    customer_name,
    tier,
    signup_date
from {{ ref('stg_customers') }}

{% if is_incremental() %}
where customer_id > (select coalesce(max(customer_id), 0) from {{ this }})
{% endif %}
