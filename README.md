# African Economic & Trade Analytics Pipeline

A data engineering portfolio project that ingests, models, and
historizes economic indicators for 54 African countries from the World Bank's
World Development Indicators (WDI) API, with UN Comtrade trade data (exports/
imports) planned as a second source. It is built to demonstrate multi-source
integration, dimensional modeling, and asset-based orchestration.

## Tech stack

- **Ingestion**: Python (`requests`, retry/backoff, idempotent full-replace landing)
- **Orchestration**: Dagster + `dagster-dbt`
- **Transformation**: dbt-core (`dbt-postgres`)
- **Warehouse**: ClickHouse (Docker), medallion schema layout (`bronze` / `silver` / `gold`)
- **BI**: Power BI, Tableau

## Data scope

- 54 African sovereign states, defined as a static, hardcoded
ISO3 list rather than derived from WDI's own region field. 

- 26 curated WDI indicators spanning macro/growth, trade
balance, demographics, labor, prices, health, education, infrastructure, and
agriculture.

- 1990–2025 for indicator observations; income classification
history extends back to 1987 (see below).

## Key design decisions

Raw API responses land in `bronze` as untouched JSONB so that any transformation bug is visible and fixable in dbt SQL.

Each run does a full delete-and-reinsert per indicator/entity. Re-running produces identical state as it does not accumulate duplicate or historical raw snapshots.

**`dim_country` is a genuinely historized SCD2 dimension**, as  accumulates history *from the date it first runs forward*. It cannot know about a reclassification that happened in 2011. To give the dimension real pre-project history, this pipeline:

1. Backfills official World Bank income-classification history (1987–present,
   sourced via Our World in Data's mirror of the same dataset), collapsed into closed date ranges ending at the live snapshot's first-run date.
2. Runs a live dbt **snapshot** (`check` strategy on `income_level_code`/
   `income_level_name` only — not on near-static fields like capital city)
   that owns everything from that date forward, using an open (`NULL`)
   upper bound.
3. Unions both in the `dim_country` mart, converts the snapshot's `NULL`
   upper bound to a `9999-12-31` sentinel (avoiding `NULL`-comparison
   pitfalls in downstream date-range queries), and applies a gaps-and-islands
   compaction pass to collapse any adjacent identical-value ranges introduced
   artificially at the seed/snapshot join point.

## Running locally

```bash
docker compose up -d
cd dbt_project && uv run dbt seed && uv run dbt snapshot && uv run dbt build
cd ../ingestion && uv run python wdi_client.py
cd ../dagster_project && uv run dagster dev
```