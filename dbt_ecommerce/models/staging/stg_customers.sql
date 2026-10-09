with source as (
    select * from {{ source('bronze_raw', 'raw_customers') }}
),

renamed as (
    select
        "id_customer" as id_customer,
        trim("full_name") as full_name,
        lower(trim("email")) as email,
        "cpf" as cpf,
        "address" as address,
        "city" as city,
        upper("state") as state,
        "zip_code" as zip_code,
        "created_at" as created_at,
        "updated_at" as updated_at,
        "_extracted_at" as _extracted_at
    from source
)

select * from renamed