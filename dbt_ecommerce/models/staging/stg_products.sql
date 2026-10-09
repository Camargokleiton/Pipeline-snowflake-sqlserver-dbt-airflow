with source as (
    select * from {{ source('bronze_raw', 'raw_products') }}
),

renamed as (
    select
        "id_product" as id_product,
        upper(trim("sku")) as sku,
        trim("product_name") as product_name,
        "id_category" as id_category,
        cast("sale_price" as numeric(10,2)) as sale_price,
        cast("unit_cost" as numeric(10,2)) as unit_cost,
        "current_stock" as current_stock,
        "created_at" as created_at,
        "updated_at" as updated_at,
        "_extracted_at" as _extracted_at
    from source
)

select * from renamed