{% snapshot snapshot_products %}

{{
    config(
      target_database='ERP_DATABASE',
      target_schema='SILVER',
      unique_key='id_product',
      strategy='check',
      check_cols=[
        'sku',
        'product_name',
        'id_category',
        'sale_price',
        'unit_cost',
        'current_stock',
        'category_name',
        'category_description'
      ]
    )
}}

select
    products.*,
    categories.category_name,
    categories.description as category_description
from {{ ref('stg_products') }} as products
left join {{ ref('stg_categories') }} as categories
    on products.id_category = categories.id_category

{% endsnapshot %}