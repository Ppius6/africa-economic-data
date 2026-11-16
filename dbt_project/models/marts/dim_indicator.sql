{{ config(
    materialized='table',
    schema='marts',
    engine='MergeTree()',
    order_by=['indicator_code'],
    contract={'enforced': true}
) }}


select 
    indicator_code,
    indicator_name,
    unit,
    category
from {{ source('pg_bridge', 'stg_indicators') }}