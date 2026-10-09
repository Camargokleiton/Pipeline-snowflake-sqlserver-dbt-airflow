with snapshot_versions as (
    select
        customer.id_customer as customer_id,
        customer.full_name,
        customer.email,
        customer.cpf,
        customer.address,
        customer.city,
        customer.state,
        customer.zip_code,
        customer.created_at,
        customer.updated_at,
        customer.dbt_scd_id as customer_sk,
        customer.dbt_valid_from,
        customer.dbt_valid_to,
        min(customer.dbt_valid_from) over (
            partition by customer.id_customer
        ) as first_valid_from
    from {{ ref('snapshot_customers') }} as customer
    where customer.dbt_valid_to is null
        or customer.dbt_valid_to > customer.dbt_valid_from
)

select
    customer_sk,
    customer_id,
    full_name,
    email,
    cpf,
    address,
    city,
    state,
    zip_code,
    created_at,
    updated_at,
    case
        when dbt_valid_from = first_valid_from
            then least(coalesce(created_at, dbt_valid_from), dbt_valid_from)
        else dbt_valid_from
    end as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from snapshot_versions
