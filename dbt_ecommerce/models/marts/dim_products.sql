select
    products.id_product as product_id,
    products.sku,
    products.product_name,
    products.id_category as category_id,
    categories.category_name,
    categories.description as category_description,
    products.sale_price,
    products.unit_cost,
    products.current_stock,
    products.created_at,
    products.updated_at,
    products._extracted_at
from {{ ref('stg_products') }} as products
left join {{ ref('stg_categories') }} as categories
    on products.id_category = categories.id_category