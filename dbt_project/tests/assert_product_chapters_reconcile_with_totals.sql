-- For each year x exporter x importer, the HS chapters should add up to the
-- all-products figure in
-- fct_bilateral_trade, on each end's own report. Returns the pairs that miss by more
-- than 5% and US$1M.
--
-- About 400 pairs miss today (confidential trade and rounding), so the test only
-- warns above 500 and
-- fails above 1,000. A swapped exporter/importer in the product chain makes over
-- 100,000 pairs miss.
{{ config(tags=["clickhouse"], warn_if=">500", error_if=">1000") }}

with
    chapter_sums as (
        select
            year,
            exporter_m49,
            importer_m49,
            sum(exporter_reported_usd) as exporter_chapter_sum,
            sum(importer_reported_usd) as importer_chapter_sum
        from {{ ref("fct_bilateral_product_trade") }}
        group by year, exporter_m49, importer_m49
    ),

    compared as (
        select
            c.year as year,
            c.exporter_m49 as exporter_m49,
            c.importer_m49 as importer_m49,
            c.exporter_chapter_sum as exporter_chapter_sum,
            t.exporter_reported_usd as exporter_total,
            c.importer_chapter_sum as importer_chapter_sum,
            t.importer_reported_usd as importer_total
        from chapter_sums c
        inner join
            {{ ref("fct_bilateral_trade") }} t
            on t.year = c.year
            and t.exporter_m49 = c.exporter_m49
            and t.importer_m49 = c.importer_m49
    )

select
    'exporter' as reported_end,
    year,
    exporter_m49,
    importer_m49,
    exporter_chapter_sum as chapter_sum,
    exporter_total as total
from compared
where
    exporter_total > 0
    and abs(exporter_chapter_sum - exporter_total)
    > greatest(0.05 * exporter_total, 1e6)

union all

select
    'importer' as reported_end,
    year,
    exporter_m49,
    importer_m49,
    importer_chapter_sum as chapter_sum,
    importer_total as total
from compared
where
    importer_total > 0
    and abs(importer_chapter_sum - importer_total)
    > greatest(0.05 * importer_total, 1e6)
