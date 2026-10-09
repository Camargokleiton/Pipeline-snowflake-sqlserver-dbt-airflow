with source as (
    select * from {{ source('bronze_raw', 'raw_order_items') }}
),

renamed as (
    select
        "id_item" as id_item,
        "id_order" as id_order,
        "id_product" as id_product,
        "quantity" as quantity,
        cast("unit_price" as numeric(10,2)) as unit_price,
        cast(("quantity" * "unit_price") as numeric(10,2)) as subtotal,
        "_extracted_at" as _extracted_at
    from source
)

select * from renamed