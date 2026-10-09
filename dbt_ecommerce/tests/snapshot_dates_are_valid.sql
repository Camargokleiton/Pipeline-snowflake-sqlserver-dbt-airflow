select
    'snapshot_customers' as snapshot_name,
    id_customer as record_id
from {{ ref('snapshot_customers') }}
where year(dbt_updated_at) not between 1900 and 2100
    or year(dbt_valid_from) not between 1900 and 2100
    or (dbt_valid_to is not null and year(dbt_valid_to) not between 1900 and 2100)
    or (created_at is not null and year(created_at) not between 1900 and 2100)
    or (updated_at is not null and year(updated_at) not between 1900 and 2100)

union all

select
    'snapshot_products',
    id_product
from {{ ref('snapshot_products') }}
where year(dbt_updated_at) not between 1900 and 2100
    or year(dbt_valid_from) not between 1900 and 2100
    or (dbt_valid_to is not null and year(dbt_valid_to) not between 1900 and 2100)
    or (created_at is not null and year(created_at) not between 1900 and 2100)
    or (updated_at is not null and year(updated_at) not between 1900 and 2100)
