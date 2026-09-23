{% snapshot snapshot_products %}

{{
    config(
      target_database='ERP_ECOMMERCE_DW',
      target_schema='SILVER_TRANSFORMED',
      unique_key='id_product',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}