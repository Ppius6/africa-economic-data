# African Economic & Trade Analytics Pipeline

An end-to-end data engineering pipeline for ingesting, transforming, and historizing economic indicators across 54 African countries using the World Bank’s World Development Indicators (WDI) API.

The architecture uses PostgreSQL for the bronze and silver layers and ClickHouse for the gold layer. Instead of duplicating silver data into ClickHouse, the gold layer queries the silver layer through a live cross-database bridge, keeping the analytical layer current without an additional data-copy step.

See **Future directions** below for planned improvements and extensions.

## Tech stack

- **Ingestion**: Python (`requests`), retry/backoff, full-replace landing per indicator
- **Transformation**: dbt-core, split across two adapters: `dbt-postgres` for bronze/silver, `dbt-clickhouse` for gold
- **Warehouses**: Postgres (bronze, silver) and ClickHouse (gold), both in Docker
- **Orchestration**: Dagster + `dagster-dbt`, one asset graph spanning both adapters
- **BI**: Power BI

## Data scope

54 African sovereign states, defined as a static, hardcoded ISO3 list rather
than derived from WDI's own region field. WDI has reclassified countries
between its Sub-Saharan Africa and Middle East/North Africa groupings before,
and a fixed list avoids depending on that.

26 WDI indicators spanning growth, trade, demographics, labor, prices,
health, education, infrastructure, and agriculture. Indicator observations
run 1990–2025; income classification history reaches back to 1987 (see
below for why the two dates differ).

## Key design decisions

Raw API responses land in `bronze` as untouched JSONB, so a transformation
bug shows up in dbt SQL, not in the ingestion script.

Each ingestion run deletes and reinserts per indicator. Re-running produces
the same end state every time and nothing accumulates duplicate or stale rows
from a prior run.

**`dim_country` is a historized SCD2 dimension.** A dbt snapshot
only ever starts recording history from the day it first runs, so on its own
it can't know that a country's income bracket changed in 2011. This pipeline
fixes that in three steps:

1. A seed backfills the World Bank's own income-classification history
   (1987–present, sourced through Our World in Data's mirror of the same
   dataset), reshaped into closed date ranges ending at the snapshot's
   first-run date.
2. A dbt snapshot (`check` strategy on `income_level_code`/`income_level_name`
   only, not on near-static fields like capital city) owns everything from
   that date forward, with an open upper bound.
3. `dim_country` unions both, converts the snapshot's open bound to a
   `2299-12-31` sentinel, and runs a gaps-and-islands compaction pass to
   collapse any adjacent identical-value ranges the seed/snapshot join point
   would otherwise split apart artificially.

**Every WDI indicator value is historized too, the same way.** A second dbt
snapshot (`fct_indicator_value_snapshot`) tracks `value` per
`country_code`/`indicator_code`/`year`, so when the World Bank revises a
figure, both the old and new values stay on record with their own validity
windows. `fact_economic_indicator` reads only the currently open row, so
revisions never silently overwrite what the pipeline previously reported.

**Gold is built through a live bridge.** ClickHouse's
`PostgreSQL` table engine defines read-through tables (`silver_bridge.*`)
that forward every query straight to the real Postgres tables. dbt's gold
models read these through `source()` rather than `ref()`, since a dbt run
can't `ref()` across two adapters. Nothing is duplicated between the two
warehouses; ClickHouse just queries Postgres live whenever gold gets rebuilt.

## Running locally

```bash
docker compose up -d          # starts both Postgres and ClickHouse
cp .env.example .env          # fill in real credentials

# ingestion must run first, since bronze has to exist before anything reads it
cd ingestion && uv run python wdi_client.py

# bronze/silver: staging views, the seed, and both snapshots, all in Postgres
cd ../dbt_project
uv run dbt build --target dev --exclude dim_country dim_indicator fact_economic_indicator

# gold: the three marts, built in ClickHouse against the silver bridge
uv run dbt build --target gold_clickhouse --select dim_country dim_indicator fact_economic_indicator

# orchestration (needs the -m flag to find the project)
cd ../dagster_project && uv run dagster dev -m dagster_project.definitions
```

A weekly schedule (`full_refresh_schedule`, Mondays 6am) is defined in
`dagster_project/dagster_project/schedules.py` and covers the whole pipeline
in one job. It starts paused, so toggle it on from the Dagster UI once
you've confirmed a manual run succeeds.

## Future directions

The project could be expanded to include this data:

**UN Comtrade trade data** (exports/imports) is the planned second data
source. It needs an ISO3-to-M49 country code crosswalk before it can join
against the existing dimensions, plus a decision on how to handle
mirror-statistics discrepancies (the same trade flow reported differently by
the two countries on either end of it).

**The Africa Visa Openness Index** would need semi-manual extraction, since
the AfDB/AU source data is published as PDF reports with no API.

**An African transport systems database** (roads, air routes) is structurally
a graph problem, not a country-year table. It would need an edge-list fact
table and likely `networkx` for connectivity metrics. 