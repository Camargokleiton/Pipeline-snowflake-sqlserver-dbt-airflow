{% snapshot snapshot_customers %}

{{
    config(
      target_database='ERP_DATABASE',
      target_schema='SILVER',
      unique_key='id_customer',
      strategy='check',
      check_cols=[
        'full_name',
        'email',
        'cpf',
        'address',
        'city',
        'state',
        'zip_code',
        'created_at',
        'updated_at'
      ]
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
