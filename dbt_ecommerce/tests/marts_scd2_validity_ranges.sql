select
    'customers' as dimension_name,
    customer_id as business_key,
    valid_from,
    valid_to
from {{ ref('dim_customers') }}
where valid_to is not null
    and valid_to <= valid_from

union all

select
    'products' as dimension_name,
    product_id as business_key,
    valid_from,
    valid_to
from {{ ref('dim_products') }}
where valid_to is not null
    and valid_to <= valid_from
