{% snapshot snapshot_customers %}

{{
    config(
      target_database='ERP_DATABASE',
      target_schema='SILVER',
      unique_key='id_customer',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
