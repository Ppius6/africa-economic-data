{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['country_code', 'indicator_code', 'year']
) }}

with indicator_values as (
    select
        country_code,
        indicator_code,
        year,
        value
    from {{ source('silver_bridge', 'fct_indicator_value_snapshot') }}
    where dbt_valid_to is null
),

-- Attach each observation to the country's income classification as it
-- stood in that observation's year — not its current classification.
-- Treats each year as Jan 1 of that year for the range comparison.
country_as_of_year as (
    select
        iv.country_code,
        iv.indicator_code,
        iv.year,
        iv.value,
        dc.income_level_name as income_level_at_time,
        dc.region_name,
        dc.country_name
    from indicator_values iv
    asof left join {{ ref('dim_country') }} dc
        on iv.country_code = dc.country_code
        and toDateTime64(makeDate(iv.year, 1, 1), 6) >= dc.valid_from
)

select
    country_code,
    country_name,
    region_name,
    indicator_code,
    year,
    value,
    income_level_at_time
from country_as_of_year