with source as (
    select * from {{ source('bronze_raw', 'raw_order_items') }}
),

renamed as (
    select
        id_item,
        id_order,
        id_product,
        quantity,
        cast(unit_price as numeric(10,2)) as unit_price,
        cast((quantity * unit_price) as numeric(10,2)) as subtotal,
        _extracted_at
    from source
)

select * from renamed