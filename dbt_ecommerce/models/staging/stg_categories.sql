with source as (
    select * from {{ source('bronze_raw', 'raw_categories') }}
),

renamed as (
    select
        id_category,
        trim(category_name) as category_name,
        trim(description) as description,
        _extracted_at
    from source
)

select * from renamed