-- One row per HS code at every level (TOTAL, 2-digit chapters, 4-digit headings, 6-digit subheadings),
-- flattened from Comtrade's HS reference. Descriptions are the H6 (2022) edition's wording.

with source as (
    select *
    from {{ source('raw', 'comtrade_hs_raw') }}
)

select
    cmd_code as hs_code,
    regexp_replace(raw_record ->> 'text', '^\S+ - ', '') as hs_description,
    (raw_record ->> 'aggrLevel')::int as aggr_level,
    nullif(raw_record ->> 'parent', '#') as parent_hs_code,
    (raw_record ->> 'isLeaf') = '1' as is_leaf,
    ingested_at
from source