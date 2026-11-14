-- One row per Comtrade area code (M49), from the partner-area reference, which also covers
-- every reporter code. Maps codes to ISO3 and marks which are in the pipeline's scope.
--
-- Codes are many-to-one onto ISO3: Ethiopia is 230 (to 1992) and 231, Sudan is 736 (to mid-2011)
-- and 729. Each code only carries data for the years it was valid, so a direct code -> ISO3
-- mapping needs no date logic. Note that 736 (Sudan before July 2011) includes what is now South Sudan.
--
-- 711, the Southern African Customs Union, reported jointly for South Africa, Botswana, Lesotho,
-- Namibia and Eswatini until 1999. It cannot be split by country, so it is its own in-scope area.

with source as (
    select * from {{ source('bronze', 'comtrade_partner_areas_raw') }}
),

african_countries as (
    select country_code from {{ ref('stg_countries') }}
),

flattened as (
    select
        partner_code as m49_code,
        raw_record ->> 'text' as area_name,
        nullif(trim(raw_record ->> 'PartnerCodeIsoAlpha3'), '') as comtrade_iso3,
        (raw_record ->> 'isGroup')::boolean as is_group,
        (raw_record ->> 'entryEffectiveDate')::date as valid_from,
        (raw_record ->> 'entryExpiredDate')::date as valid_to,
        ingested_at
    from source
),

typed as (
    select
        *,
        case
            when m49_code = 0 then 'world'
            when m49_code = 711 then 'customs_union'
            when is_group then 'group'
            when comtrade_iso3 ~ '^[A-Z]{3}$' then 'country'
            else 'other_area'
        end as area_type
    from flattened
)

select
    t.m49_code,
    t.area_name,
    t.area_type,
    case when t.area_type = 'country' then t.comtrade_iso3 end as iso3,
    (a.country_code is not null or t.area_type = 'customs_union') as is_in_scope,
    t.valid_from,
    t.valid_to,
    t.ingested_at
from typed t
left join african_countries a
    on t.area_type = 'country' and a.country_code = t.comtrade_iso3
