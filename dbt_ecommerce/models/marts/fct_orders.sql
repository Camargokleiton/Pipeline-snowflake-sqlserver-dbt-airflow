with order_item_totals as (
    select
        id_order,
        count(*) as order_line_count,
        sum(quantity) as total_quantity,
        sum(subtotal) as items_subtotal
    from {{ ref('stg_order_items') }}
    group by id_order
),

payment_totals as (
    select
        id_order,
        max(payment_method) as payment_method,
        max(payment_status) as payment_status,
        sum(amount) as payment_amount,
        max(payment_date) as payment_date
    from {{ ref('stg_payments') }}
    group by id_order
)

select
    orders.id_order as order_id,
    orders.id_customer as customer_id,
    orders.order_date,
    orders.current_status as order_status,
    orders.total_value as order_total,
    orders.shipping_value,
    coalesce(order_item_totals.order_line_count, 0) as order_line_count,
    coalesce(order_item_totals.total_quantity, 0) as total_quantity,
    coalesce(order_item_totals.items_subtotal, 0) as items_subtotal,
    payment_totals.payment_method,
    payment_totals.payment_status,
    payment_totals.payment_amount,
    payment_totals.payment_date,
    orders.updated_at,
    orders._extracted_at
from {{ ref('stg_orders') }} as orders
left join order_item_totals
    on orders.id_order = order_item_totals.id_order
left join payment_totals
    on orders.id_order = payment_totals.id_order