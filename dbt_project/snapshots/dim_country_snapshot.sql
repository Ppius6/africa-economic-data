{% snapshot dim_country_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='country_code',
        strategy='check',
        check_cols=['income_level_code', 'income_level_name']
    )
}}

select
    country_code,
    country_name,
    iso2_code,
    region_code,
    region_name,
    income_level_code,
    income_level_name,
    lending_type,
    capital_city,
    longitude,
    latitude
from {{ ref('stg_countries') }}

{% endsnapshot %}