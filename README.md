# African Economic & Trade Analytics Pipeline

An end-to-end data engineering pipeline for ingesting, transforming, and historizing economic indicators across 54 African countries using the World Bank’s World Development Indicators (WDI) API.

The architecture uses PostgreSQL for the bronze and silver layers and ClickHouse for the gold layer. Instead of duplicating silver data into ClickHouse, the gold layer queries the silver layer through a live cross-database bridge, keeping the analytical layer current without an additional data-copy step.

## Tech stack

- **Ingestion**: Python
- **Transformation**: dbt-core (`dbt-postgres` for bronze/silver, `dbt-clickhouse` for gold)
- **Warehouses**: Postgres (bronze, silver) and ClickHouse (gold), in Docker
- **Orchestration**: Dagster
- **BI**: Power BI

## Architecture

![Project Architecture](WDI%20architecture@1x.png)

## Documentation

Column-level docs, lineage graph, and test coverage:
**[ppius6.github.io/africa-economic-data](https://ppius6.github.io/africa-economic-data/)**

The three gold marts carry an enforced dbt contract, so a schema change that doesn't match the declared column types fails the build.

## Data scope

- 54 African sovereign states, as a fixed ISO3 list.
- 26 WDI indicators covering growth, trade, demographics, labor, prices, health, education, infrastructure, and agriculture.
- Indicator observations run from 1990 to the latest year WDI has published, with Income classification history starting in 1987.

## Key design decisions

- **Bronze is raw.** API responses land as untouched JSONB.
- **Ingestion is idempotent.** Each run deletes and reinserts per indicator, so re-runs never leave duplicate or stale rows.
- **History is kept.**
  - `dim_country` is an SCD2 dimension. A seed backfills the World Bank's income-classification history (1987 onward), and a dbt snapshot tracks changes from its first run. `dim_country` unions the two.
  - Indicator values are snapshotted too, so when the World Bank revises a figure, the old and new values both stay on record. The fact table reads only the current row.
- **Gold reads silver through a live bridge.** ClickHouse's `PostgreSQL` table engine exposes silver as `silver_bridge.*`, and gold models read it via `source()`, since dbt can't `ref()` across adapters. No data is copied.

## Running locally

1. Copy the environment file and start everything:

   ```bash
   cp .env.example .env
   docker compose up -d
   ```

   The first run builds the image and takes a few minutes.

2. Open the Dagster UI at <http://localhost:3100>. To use another port, set `DAGSTER_PORT` in `.env`.

3. Go to Overview → Jobs → `full_refresh` and click Launch Run. This builds bronze, silver, then gold in order.

To stop: 

- `docker compose down` 

Or delete everything:
- `docker compose down -v` deletes it.

After changing anything, run `docker compose up -d --build`.

### Optional

- **Single asset:** Overview → Assets, select it, click Materialize selected. Use the graph view to see the full dependency chain.
- **Weekly schedule:** `full_refresh_schedule` (Mondays 6am) starts paused. Turn it on under Overview → Schedules after a manual run succeeds.
- **Without Dagster:** with the databases running and `uv sync` done, run these in order:

  ```bash
  cd ingestion && uv run python -m wdi.pipeline
  cd ../dbt_project
  uv run dbt build --target dev --exclude dim_country dim_indicator fact_economic_indicator
  uv run dbt build --target gold_clickhouse --select dim_country dim_indicator fact_economic_indicator
  ```

## Future directions

- **UN Comtrade trade data** as a second source, which needs an ISO3-to-M49 country code crosswalk.
- **Africa Visa Openness Index**, which needs semi-manual extraction from PDF reports.
- **African transport network** (roads, air routes), modeled as an edge-list fact table.
