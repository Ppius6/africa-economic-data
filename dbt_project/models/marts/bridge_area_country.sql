{{
    config(
        materialized='table',
        schema='marts',
        engine='MergeTree()',
        order_by=['m49_code', 'iso3'],
        contract={'enforced': true}
        )
}}

-- Area -> country. One row per country an area stands for: most areas map to one country,
-- several codes can map to the same country (Sudan 736/729), and 711 maps to five.

select
    m49_code,
    cast(arrayJoin(member_iso3) as LowCardinality(String)) as iso3,
    cast(length(member_iso3) > 1 as Bool) as is_shared_area,
    is_in_scope
from {{ ref('dim_trade_area') }}