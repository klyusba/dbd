select
    o.order_id,
    o.order_date,
    o.quantity,
    c.customer_id,
    c.customer_name,
    c.tier,
    c.signup_date,
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    (o.quantity * p.unit_price)                        as line_total
from {{ ref('stg_orders') }}    as o
join {{ ref('stg_customers') }} as c on o.customer_id = c.customer_id
join {{ ref('stg_products') }}  as p on o.product_id  = p.product_id
