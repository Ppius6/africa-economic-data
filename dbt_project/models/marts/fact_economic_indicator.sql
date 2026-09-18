with indicator_values as (
    select
        country_code,
        indicator_code,
        year,
        value
    from {{ ref('stg_wdi_indicators') }}
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
    left join {{ ref('dim_country') }} dc
        on iv.country_code = dc.country_code
        and make_date(iv.year, 1, 1) >= dc.valid_from
        and make_date(iv.year, 1, 1) < dc.valid_to
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