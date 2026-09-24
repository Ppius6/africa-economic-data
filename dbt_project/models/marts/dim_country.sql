{{
    config(
        materialized='table', 
        schema='gold', 
        engine='MergeTree()',
        order_by=['country_code', 'valid_from']
        )
}}

with historical_income as (
    select
        country_code,
        income_level_name,
        valid_from::timestamp as valid_from,
        valid_to::timestamp as valid_to
    from {{ source('silver_bridge', 'income_classification_history') }}
),

current_and_future_income as (
    select
        country_code,
        income_level_name,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, CAST('2299-12-31' AS DateTime64(6))) AS valid_to
    from {{ source('silver_bridge', 'dim_country_snapshot') }}
),

income_history as (
    select * from historical_income
    union all
    select * from current_and_future_income
),

flagged as (
    select 
        *,
        case 
            when income_level_name != lag(income_level_name) over (
                partition by country_code order by valid_from
            ) then 1
            when lag(income_level_name) over (
                partition by country_code order by valid_from
            ) is null then 1
            else 0
        end as is_new_island
    from income_history
),

grouped as (
    select 
        *,
        sum(is_new_island) over (
            partition by country_code order by valid_from
        ) as island_id
    from flagged
),

compacted as (
    select
        country_code,
        income_level_name,
        min(valid_from) as valid_from,
        max(valid_to) as valid_to
    from grouped
    group by country_code, island_id, income_level_name
),

current_attributes as (
    select
        country_code,
        country_name,
        iso2_code,
        region_code,
        region_name,
        lending_type,
        capital_city,
        longitude,
        latitude
    from {{ source('silver_bridge', 'stg_countries') }}
)

select
    c.country_code,
    a.country_name,
    a.iso2_code,
    a.region_code,
    a.region_name,
    c.income_level_name,
    a.lending_type,
    a.capital_city,
    a.longitude,
    a.latitude,
    c.valid_from,
    c.valid_to,
    (c.valid_to = CAST('2299-12-31' AS DateTime64(6))) as is_current
from compacted c
left join current_attributes a using (country_code)
order by c.country_code, c.valid_from