select
    order_id,
    ((order_id - 1) % 100) + 1                        as customer_id,
    ((order_id - 1) % 20)  + 1                        as product_id,
    (order_id % 5) + 1                                 as quantity,
    date '2024-01-01' + ((order_id - 1) % 365)        as order_date
from generate_series(1, 500) as t(order_id)
