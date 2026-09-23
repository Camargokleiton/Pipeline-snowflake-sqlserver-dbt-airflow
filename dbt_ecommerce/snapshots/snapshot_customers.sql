{% snapshot snapshot_customers %}

{{
    config(
      target_database='ERP_ECOMMERCE_DW',
      target_schema='SILVER_TRANSFORMED',
      unique_key='id_customer',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
