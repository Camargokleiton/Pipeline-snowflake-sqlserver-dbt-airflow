with dimension_versions as (
    select
        'customers' as dimension_name,
        customer_id as business_key,
        customer_sk as version_key,
        valid_from,
        coalesce(valid_to, '9999-12-31 00:00:00'::timestamp_ntz) as valid_to
    from {{ ref('dim_customers') }}

    union all

    select
        'products' as dimension_name,
        product_id as business_key,
        product_sk as version_key,
        valid_from,
        coalesce(valid_to, '9999-12-31 00:00:00'::timestamp_ntz) as valid_to
    from {{ ref('dim_products') }}
)

select
    current_version.dimension_name,
    current_version.business_key,
    current_version.version_key as first_version,
    overlapping_version.version_key as overlapping_version
from dimension_versions as current_version
inner join dimension_versions as overlapping_version
    on current_version.dimension_name = overlapping_version.dimension_name
    and current_version.business_key = overlapping_version.business_key
    and current_version.version_key < overlapping_version.version_key
    and current_version.valid_from < overlapping_version.valid_to
    and overlapping_version.valid_from < current_version.valid_to
