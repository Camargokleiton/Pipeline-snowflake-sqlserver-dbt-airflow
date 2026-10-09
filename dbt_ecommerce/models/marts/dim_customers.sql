select
    id_customer as customer_id,
    full_name,
    email,
    cpf,
    address,
    city,
    state,
    zip_code,
    created_at,
    updated_at,
    _extracted_at
from {{ ref('stg_customers') }}