{{ config(
    materialized='table',
    schema='marts',
    engine='MergeTree()',
    order_by=['country_code', 'indicator_code', 'year'],
    contract={'enforced': true}
) }}

with indicator_values as (
    select
        country_code,
        indicator_code,
        year,
        value
    from {{ source('pg_bridge', 'fct_indicator_value_snapshot') }}
    where dbt_valid_to is null
),

-- Name and region do not change with the year, so they come from each
-- country's current row and every observation finds a match.

current_country as (
    select
        country_code,
        country_name,
        region_name
    from {{ ref('dim_country') }}
    where is_current
),

-- The income level is cast to Nullable before the join. ClickHouse fills an
-- unmatched join row with the column's default value, which is '' for String
-- but NULL for Nullable(String), so a year before the country's first
-- classification (DJI 1990, ERI 1990-1992, SSD 1990-2011) comes out as NULL.

income_periods as (
    select
        country_code,
        valid_from,
        cast(income_level_name as Nullable(String)) as income_level_name
    from {{ ref('dim_country') }}
)

-- Each observation is attached to the income classification as it stood in
-- that observation's year, not its current one. Each year is treated as
-- 1 January of that year for the comparison.

select
    iv.country_code as country_code,
    cc.country_name,
    cc.region_name,
    iv.indicator_code,
    iv.year,
    iv.value,
    ip.income_level_name as income_level_at_time
from indicator_values iv
left join current_country cc
    on cc.country_code = iv.country_code
asof left join income_periods ip
    on iv.country_code = ip.country_code
    and toDateTime64(makeDate(iv.year, 1, 1), 6) >= ip.valid_from