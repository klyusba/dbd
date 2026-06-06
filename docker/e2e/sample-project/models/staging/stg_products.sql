select
    product_id,
    'Product ' || product_id                           as product_name,
    case ((product_id - 1) % 4)
        when 0 then 'electronics'
        when 1 then 'clothing'
        when 2 then 'food'
        else        'home'
    end                                                as category,
    (10 + product_id * 5)::numeric                     as unit_price
from generate_series(1, 20) as t(product_id)
