-- One row per reporter x partner x flow x year, flattened from bronze's raw Comtrade records.
-- All products combined (HS TOTAL), aggregate rows only (no customs, transport or second-partner split).

with source as (
    select * from {{ source('bronze', 'comtrade_raw') }}
),

flattened as (
    select
        (raw_record ->> 'refYear')::int as year,
        (raw_record ->> 'reporterCode')::int as reporter_m49,
        (raw_record ->> 'partnerCode')::int as partner_m49,
        raw_record ->> 'flowCode' as flow_code,
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

-- A record between two in-scope areas comes back from both pulls (reporter side and partner side).
-- The two copies are identical, so keep one.
deduplicated as (
    select distinct on (year, reporter_m49, partner_m49, flow_code) *
    from flattened
    order by year, reporter_m49, partner_m49, flow_code, query_side
)

select
    year,
    reporter_m49,
    partner_m49,
    flow_code,
    trade_value_usd,
    cif_value_usd,
    fob_value_usd,
    net_weight_kg,
    hs_edition,
    is_reported,
    legacy_estimation_flag,
    ingested_at
from deduplicated
