with source as (
    select * from {{ source('bronze', 'country_raw') }}
),

flattened as (
    select
        country_code,
        raw_record ->> 'name' as country_name,
        raw_record ->> 'iso2Code' as iso2_code,
        raw_record -> 'region' ->> 'id' as region_code,
        trim(raw_record -> 'region' ->> 'value') as region_name,
        raw_record -> 'incomeLevel' ->> 'id' as income_level_code,
        raw_record -> 'incomeLevel' ->> 'value' as income_level_name,
        raw_record -> 'lendingType' ->> 'value' as lending_type,
        nullif(raw_record ->> 'capitalCity', '') as capital_city,
        nullif(raw_record ->> 'longitude', '')::float as longitude,
        nullif(raw_record ->> 'latitude', '')::float as latitude,
        ingested_at
    from source
)

select * from flattened