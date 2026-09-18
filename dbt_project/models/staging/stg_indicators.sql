with source as (
    select *
    from {{ source('bronze', 'indicator_raw') }}
),

first_topic as (
    select
        indicator_code,
        raw_record ->> 'name' as indicator_name,
        nullif(raw_record ->> 'unit', '') as unit,
        (
            select topic ->> 'value'
            from jsonb_array_elements(raw_record -> 'topics') as topic
            limit 1
        ) as category
    from source
)

select * from first_topic