with source as (
    select * from {{ source('bronze_raw', 'raw_customers') }}
),

renamed as (
    select
        id_customer,
        trim(full_name) as full_name,
        lower(trim(email)) as email,
        cpf,
        address,
        city,
        upper(state) as state,
        zip_code,
        created_at,
        updated_at,
        _extracted_at
    from source
)

select * from renamed