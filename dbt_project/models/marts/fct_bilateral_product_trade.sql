{{
    config(
        materialized='table',
        schema='marts',
        engine='MergeTree()',
        order_by=['year', 'hs_chapter', 'exporter_m49', 'importer_m49'],
        contract={'enforced':true}
    )
}}

-- One row per year x exporter x importer x HS chapter (2000 onwards), with both ends' reported figures.
-- Join exporter_m49 and importer_m49 to dim_trade_area, and hs_chapter to dim_hs_chapter, for names.

select
    year,
    exporter_m49,
    importer_m49,
    cast(hs_chapter as LowCardinality(String)) as hs_chapter,
    exporter_reported_usd,
    importer_reported_usd,
    cast(importer_value_basis as LowCardinality(Nullable(String))) as importer_value_basis,
    cast(reported_by as LowCardinality(String)) as reported_by,
    is_intra_african,
    cast(exporter_hs_edition as LowCardinality(Nullable(String))) as exporter_hs_edition,
    cast(importer_hs_edition as LowCardinality(Nullable(String))) as importer_hs_edition
from {{ source('pg_bridge', 'int_comtrade_product_flows') }}