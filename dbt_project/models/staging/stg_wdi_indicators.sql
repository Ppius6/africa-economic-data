with source as (
    select * from {{ source('bronze', 'wdi_raw') }}
),

flattened as (
    select
        raw_record ->> 'countryiso3code' as country_code,
        raw_record -> 'indicator' ->> 'id' as indicator_code,
        raw_record -> 'indicator' ->> 'value' as indicator_name,
        (raw_record ->> 'date')::int as year,
        (raw_record ->> 'value')::numeric as value,
        ingested_at
    from source
)

select * from flattened
