select
    'stg_customers.created_at' as field_name,
    id_customer as record_id
from {{ ref('stg_customers') }}
where created_at is null
    or year(created_at) not between 1900 and 2100

union all

select
    'stg_customers.updated_at',
    id_customer
from {{ ref('stg_customers') }}
where updated_at is null
    or year(updated_at) not between 1900 and 2100

union all

select
    'stg_customers._extracted_at',
    id_customer
from {{ ref('stg_customers') }}
where _extracted_at is null
    or year(_extracted_at) not between 1900 and 2100

union all

select
    'stg_products.created_at',
    id_product
from {{ ref('stg_products') }}
where created_at is null
    or year(created_at) not between 1900 and 2100

union all

select
    'stg_products.updated_at',
    id_product
from {{ ref('stg_products') }}
where updated_at is null
    or year(updated_at) not between 1900 and 2100

union all

select
    'stg_products._extracted_at',
    id_product
from {{ ref('stg_products') }}
where _extracted_at is null
    or year(_extracted_at) not between 1900 and 2100

union all

select
    'stg_orders.order_date',
    id_order
from {{ ref('stg_orders') }}
where order_date is null
    or year(order_date) not between 1900 and 2100

union all

select
    'stg_orders.updated_at',
    id_order
from {{ ref('stg_orders') }}
where updated_at is null
    or year(updated_at) not between 1900 and 2100

union all

select
    'stg_orders._extracted_at',
    id_order
from {{ ref('stg_orders') }}
where _extracted_at is null
    or year(_extracted_at) not between 1900 and 2100

union all

select
    'stg_payments.payment_date',
    id_payment
from {{ ref('stg_payments') }}
where (
        payment_date is not null
        and year(payment_date) not between 1900 and 2100
    )
    or (payment_date is null and payment_status <> 'PENDING')

union all

select
    'stg_payments._extracted_at',
    id_payment
from {{ ref('stg_payments') }}
where _extracted_at is null
    or year(_extracted_at) not between 1900 and 2100

union all

select
    'stg_order_items._extracted_at',
    id_item
from {{ ref('stg_order_items') }}
where _extracted_at is null
    or year(_extracted_at) not between 1900 and 2100

union all

select
    'stg_categories._extracted_at',
    id_category
from {{ ref('stg_categories') }}
where _extracted_at is null
    or year(_extracted_at) not between 1900 and 2100
