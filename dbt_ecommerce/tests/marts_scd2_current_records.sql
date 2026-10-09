select
    'customers' as dimension_name,
    customer_id as business_key
from {{ ref('dim_customers') }}
where is_current
group by customer_id
having count(*) > 1

union all

select
    'products' as dimension_name,
    product_id as business_key
from {{ ref('dim_products') }}
where is_current
group by product_id
having count(*) > 1
