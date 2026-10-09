select
    'dim_customers.created_at' as field_name,
    customer_sk as record_id
from {{ ref('dim_customers') }}
where created_at is null
    or year(created_at) not between 1900 and 2100

union all

select
    'dim_customers.updated_at',
    customer_sk
from {{ ref('dim_customers') }}
where updated_at is null
    or year(updated_at) not between 1900 and 2100

union all

select
    'dim_customers.valid_from',
    customer_sk
from {{ ref('dim_customers') }}
where valid_from is null
    or year(valid_from) not between 1900 and 2100

union all

select
    'dim_customers.valid_to',
    customer_sk
from {{ ref('dim_customers') }}
where valid_to is not null
    and year(valid_to) not between 1900 and 2100

union all

select
    'dim_products.created_at',
    product_sk
from {{ ref('dim_products') }}
where created_at is null
    or year(created_at) not between 1900 and 2100

union all

select
    'dim_products.updated_at',
    product_sk
from {{ ref('dim_products') }}
where updated_at is null
    or year(updated_at) not between 1900 and 2100

union all

select
    'dim_products.valid_from',
    product_sk
from {{ ref('dim_products') }}
where valid_from is null
    or year(valid_from) not between 1900 and 2100

union all

select
    'dim_products.valid_to',
    product_sk
from {{ ref('dim_products') }}
where valid_to is not null
    and year(valid_to) not between 1900 and 2100

union all

select
    'fct_orders.order_date',
    order_id
from {{ ref('fct_orders') }}
where order_date is null
    or year(order_date) not between 1900 and 2100

union all

select
    'fct_orders.updated_at',
    order_id
from {{ ref('fct_orders') }}
where updated_at is null
    or year(updated_at) not between 1900 and 2100

union all

select
    'fct_orders.payment_date',
    order_id
from {{ ref('fct_orders') }}
where payment_date is not null
    and year(payment_date) not between 1900 and 2100
