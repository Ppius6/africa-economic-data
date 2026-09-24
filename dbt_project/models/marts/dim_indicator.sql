{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['indicator_code']
) }}


select 
    indicator_code,
    indicator_name,
    unit,
    category
from {{ source('silver_bridge', 'stg_indicators') }}