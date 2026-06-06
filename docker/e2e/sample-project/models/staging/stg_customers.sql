select
    customer_id,
    'Customer ' || customer_id                         as customer_name,
    case ((customer_id - 1) % 3)
        when 0 then 'bronze'
        when 1 then 'silver'
        else        'gold'
    end                                                as tier,
    date '2020-01-01' + (customer_id % 1000)           as signup_date
from generate_series(1, 100) as t(customer_id)
