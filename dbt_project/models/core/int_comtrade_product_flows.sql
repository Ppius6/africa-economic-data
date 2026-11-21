-- One row per year x exporter x importer x HS chapter, holding both ends' reported
-- figures side by side,
-- the same pairing as int_comtrade_bilateral_flows but split by 2-digit HS chapter
-- (2000 onwards).
-- Either figure can be missing. Choosing between them is left to a later model.
--
-- Only codes and flags are kept, not area names: at about 8M rows, names would add
-- roughly 40% to
-- every row in Postgres, and the marts can look them up in dim_trade_area.
{{ config(materialized="table") }}

with
    trade as (
        select * from {{ ref("stg_comtrade_product_trade") }} where partner_m49 <> 0  -- drop reporter totals to World; they are not a bilateral flow
    ),

    exports as (
        select
            year,
            reporter_m49 as exporter_m49,
            partner_m49 as importer_m49,
            hs_chapter,
            trade_value_usd as exporter_reported_usd,
            hs_edition as exporter_hs_edition
        from trade
        where flow_code = 'X'
    ),

    imports as (
        select
            year,
            partner_m49 as exporter_m49,
            reporter_m49 as importer_m49,
            hs_chapter,
            trade_value_usd as importer_reported_usd,
            case
                when trade_value_usd = cif_value_usd
                then 'CIF'
                when trade_value_usd = fob_value_usd
                then 'FOB'
            end as importer_value_basis,
            hs_edition as importer_hs_edition
        from trade
        where flow_code = 'M'
    ),

    imports_and_exports as (
        select *
        from exports
        full outer join imports using (year, exporter_m49, importer_m49, hs_chapter)
    ),

    areas as (select m49_code, is_in_scope from {{ ref("stg_comtrade_areas") }})

select
    ie.year,
    ie.exporter_m49,
    ie.importer_m49,
    ie.hs_chapter,
    ie.exporter_reported_usd,
    ie.importer_reported_usd,
    ie.importer_value_basis,
    case
        when
            ie.exporter_reported_usd is not null
            and ie.importer_reported_usd is not null
        then 'both'
        when ie.exporter_reported_usd is not null
        then 'exporter_only'
        else 'importer_only'
    end as reported_by,
    ex.is_in_scope as exporter_in_scope,
    im.is_in_scope as importer_in_scope,
    (ex.is_in_scope and im.is_in_scope) as is_intra_african,
    ie.exporter_hs_edition,
    ie.importer_hs_edition
from imports_and_exports ie
left join areas ex on ex.m49_code = ie.exporter_m49
left join areas im on im.m49_code = ie.importer_m49
