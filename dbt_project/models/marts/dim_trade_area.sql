{{
    config(
        materialized='table',
        schema='marts',
        engine='MergeTree()',
        order_by=['m49_code'],
        contract={'enforced': true}
        )
}}

-- One row per Comtrade area code used twice by fct_bilateral_trade as exporter and as importer
select
    m49_code,
    cast(area_name as LowCardinality(String)) as area_name,
    cast(area_type as LowCardinality(String)) as area_type,
    cast(iso3 as LowCardinality(Nullable(String))) as iso3,
    is_in_scope,
    valid_from,
    valid_to,
    cast(
        if(m49_code = 711, ['BWA', 'LSO', 'NAM', 'SWZ', 'ZAF'],
            if(iso3 is null, [], [assumeNotNull(iso3)]))
        as Array(LowCardinality(String))
    ) as member_iso3
from {{ source('pg_bridge', 'stg_comtrade_areas') }}
