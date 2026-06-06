select
    customer_id,
    customer_name,
    tier,
    signup_date,
    count(distinct order_id)     as order_count,
    sum(quantity)                as total_quantity,
    sum(line_total)              as lifetime_value,
    avg(line_total)              as avg_order_value,
    min(order_date)              as first_order_date,
    max(order_date)              as last_order_date
from {{ ref('int_orders_enriched') }}
group by customer_id, customer_name, tier, signup_date
