{% snapshot fct_indicator_value_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key=['country_code', 'indicator_code', 'year'],
        strategy='check',
        check_cols=['value']
    )
}}

select
    country_code,
    indicator_code,
    year,
    value
from {{ ref('stg_wdi_indicators') }}

{% endsnapshot %}