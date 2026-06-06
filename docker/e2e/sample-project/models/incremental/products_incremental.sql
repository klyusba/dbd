{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

select
    product_id,
    product_name,
    category,
    unit_price
from {{ ref('stg_products') }}

{% if is_incremental() %}
where product_id > (select coalesce(max(product_id), 0) from {{ this }})
{% endif %}
