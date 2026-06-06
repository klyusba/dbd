select
    product_id,
    product_name,
    category,
    unit_price,
    count(distinct order_id)     as order_count,
    count(distinct customer_id)  as unique_customers,
    sum(quantity)                as total_quantity_sold,
    sum(line_total)              as total_revenue,
    avg(line_total)              as avg_order_revenue
from {{ ref('int_orders_enriched') }}
group by product_id, product_name, category, unit_price
