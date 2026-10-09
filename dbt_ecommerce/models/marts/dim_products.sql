with snapshot_versions as (
    select
        product.id_product as product_id,
        product.sku,
        product.product_name,
        product.id_category as category_id,
        product.category_name,
        product.category_description,
        product.sale_price,
        product.unit_cost,
        product.current_stock,
        product.created_at,
        product.updated_at,
        product.dbt_scd_id as product_sk,
        product.dbt_valid_from,
        product.dbt_valid_to,
        min(product.dbt_valid_from) over (
            partition by product.id_product
        ) as first_valid_from
    from {{ ref('snapshot_products') }} as product
    where product.dbt_valid_to is null
        or product.dbt_valid_to > product.dbt_valid_from
)

select
    product_sk,
    product_id,
    sku,
    product_name,
    category_id,
    category_name,
    category_description,
    sale_price,
    unit_cost,
    current_stock,
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