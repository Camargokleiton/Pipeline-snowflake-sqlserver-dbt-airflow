with source as (
    select * from {{ source('bronze_raw', 'raw_orders') }}
),

renamed as (
    select
        id_order,
        id_customer,
        order_date,
        cast(total_value as numeric(10,2)) as total_value,
        cast(shipping_value as numeric(10,2)) as shipping_value,
        upper(trim(current_status)) as current_status,
        updated_at,
        _extracted_at
    from source
)

select * from renamed