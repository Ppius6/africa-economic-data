-- One row per reporter x partner x flow x HS chapter x year, flattened from the raw Comtrade records.
-- Same shape as stg_comtrade_trade, split by 2-digit HS chapter instead of HS TOTAL and are aggregate rows only.

with source as (
    select *
    from {{ source('raw', 'comtrade_product_raw') }}
),

flattened as (
    select
        (raw_record ->> 'refYear')::int as year,
        (raw_record ->> 'reporterCode')::int as reporter_m49,
        (raw_record ->> 'partnerCode')::int as partner_m49,
        raw_record ->> 'flowCode' as flow_code,
        raw_record ->> 'cmdCode' as hs_chapter,
        (raw_record ->> 'primaryValue')::numeric as trade_value_usd,
        (raw_record ->> 'cifvalue')::numeric as cif_value_usd,
        (raw_record ->> 'fobvalue')::numeric as fob_value_usd,
        (raw_record ->> 'netWgt')::numeric as net_weight_kg,
        raw_record ->> 'classificationCode' as hs_edition,
        (raw_record ->> 'isReported')::boolean as is_reported,
        (raw_record ->> 'legacyEstimationFlag')::int as legacy_estimation_flag,
        query_side,
        ingested_at
    from source 
),

-- A record between two in-scope areas comes back from both pulls (reporter side and partner side). The two copies are identical, so keep one.

deduplicated as (
    select distinct on (year, reporter_m49, partner_m49, flow_code, hs_chapter) *
    from flattened
    order by year, reporter_m49, partner_m49, flow_code, hs_chapter, query_side
)

select
    year,
    reporter_m49,
    partner_m49,
    flow_code,
    hs_chapter,
    trade_value_usd,
    cif_value_usd,
    fob_value_usd,
    net_weight_kg,
    hs_edition,
    is_reported,
    legacy_estimation_flag,
    ingested_at
from deduplicated