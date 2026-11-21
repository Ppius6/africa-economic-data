{{
    config(
        materialized="table",
        schema="marts",
        engine="MergeTree()",
        order_by=["hs_chapter"],
        contract={"enforced": true},
    )
}}

-- One row per 2-digit HS chapter, with the HS section it belongs to.
-- Comtrade's reference has no sections, so the 21 WCO section ranges are listed here.
-- They have been fixed since HS began in 1988. Chapter 99 is Comtrade's own code for
-- unspecified goods and sits outside the HS sections, so it gets a row of its own.
with
    chapters as (
        select hs_code, hs_description, toUInt8(hs_code) as chapter_number
        from {{ source("pg_bridge", "stg_comtrade_hs") }}
        where aggr_level = 2
    ),

    sections as (
        select *
        from
        values
            (
                'section_number UInt8, section_code String, section_name String, first_chapter UInt8, last_chapter UInt8',
                (1, 'I', 'Live animals; animal products', 1, 5),
                (2, 'II', 'Vegetable products', 6, 14),
                (3, 'III', 'Animal, vegetable or microbial fats and oils', 15, 15),
                (
                    4,
                    'IV',
                    'Prepared foodstuffs; beverages, spirits and vinegar; tobacco',
                    16,
                    24
                ),
                (5, 'V', 'Mineral products', 25, 27),
                (6, 'VI', 'Products of the chemical or allied industries', 28, 38),
                (7, 'VII', 'Plastics and rubber and articles thereof', 39, 40),
                (
                    8,
                    'VIII',
                    'Raw hides and skins, leather, furskins and articles thereof',
                    41,
                    43
                ),
                (9, 'IX', 'Wood, cork, straw and articles thereof', 44, 46),
                (
                    10,
                    'X',
                    'Pulp of wood, paper and paperboard and articles thereof',
                    47,
                    49
                ),
                (11, 'XI', 'Textiles and textile articles', 50, 63),
                (12, 'XII', 'Footwear, headgear, umbrellas, prepared feathers', 64, 67),
                (
                    13,
                    'XIII',
                    'Articles of stone, plaster, cement, ceramic and glass',
                    68,
                    70
                ),
                (
                    14,
                    'XIV',
                    'Pearls, precious stones and metals, imitation jewellery, coin',
                    71,
                    71
                ),
                (15, 'XV', 'Base metals and articles of base metal', 72, 83),
                (
                    16,
                    'XVI',
                    'Machinery and mechanical appliances; electrical equipment',
                    84,
                    85
                ),
                (
                    17,
                    'XVII',
                    'Vehicles, aircraft, vessels and associated transport equipment',
                    86,
                    89
                ),
                (
                    18,
                    'XVIII',
                    'Optical, measuring and medical instruments; clocks and watches; musical instruments',
                    90,
                    92
                ),
                (19, 'XIX', 'Arms and ammunition', 93, 93),
                (20, 'XX', 'Miscellaneous manufactured articles', 94, 96),
                (21, 'XXI', 'Works of art, collectors'' pieces and antiques', 97, 97),
                (99, 'n/a', 'Commodities not specified according to kind', 99, 99)
            )
    )

select
    c.hs_code as hs_chapter,
    c.hs_description as chapter_description,
    s.section_number as section_number,
    s.section_code as section_code,
    s.section_name as section_name
from chapters c
cross join sections s
where c.chapter_number between s.first_chapter and s.last_chapter
