with source as (
    select * from {{ source('bronze_raw', 'raw_payments') }}
),

renamed as (
    select
        id_payment,
        id_order,
        upper(trim(payment_method)) as payment_method,
        upper(trim(payment_status)) as payment_status,
        cast(amount as numeric(10,2)) as amount,
        payment_date,
        _extracted_at
    from source
)

select * from renamed