import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from dagster import AssetExecutionContext, AssetKey, AssetOut, MaterializeResult, MetadataValue, multi_asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

# Load .env into this process's actual environment at import time, rather than relying
# on whoever launches `dagster dev` to have sourced it into their shell first. dbt's own
# CLI auto-loads .env per-invocation, but dagster-dbt sometimes creates a dbt adapter
# handle directly in-process (e.g. for column metadata introspection) — that path calls
# dbt's internals directly and never goes through the CLI entrypoint that does the
# auto-loading, so it only ever sees whatever's already in os.environ for this process.
load_dotenv(Path(__file__).parent.joinpath("../../.env").resolve())

RELATIVE_PATH_TO_DBT_PROJECT = "../../dbt_project"

dbt_project = DbtProject(
    project_dir = Path(__file__).parent.joinpath(RELATIVE_PATH_TO_DBT_PROJECT).resolve(),
)

dbt_project.prepare_if_dev()

# The ClickHouse-side `silver_bridge` source tables are pass-through reads of tables
# actually built by the Postgres-side dbt assets (models/seed/snapshot). dbt's own
# manifest has no edge between them — a source() reference is always a leaf node to
# dbt, and it has no way of knowing a "source" on one adapter is secretly the same data
# as a "model" built on another. Worse, dagster-dbt never builds a real, translator-
# processed spec for "source" nodes at all (see ASSET_RESOURCE_TYPES in
# dagster_dbt/utils.py — sources are excluded), so overriding the source's own spec is a
# dead end regardless of `select`/`exclude`. Instead, this translator adds the missing
# dependency directly onto the three marts models' own specs, pointing straight at the
# real Postgres-side asset keys, so materializing the gold marts is correctly ordered
# after the silver layer they actually depend on.
_EXTRA_SILVER_DEPS = {
    "model.dbt_project.dim_country": [
        AssetKey(["silver", "stg_countries"]),
        AssetKey(["silver", "income_classification_history"]),
        AssetKey(["dim_country_snapshot"]),
    ],
    "model.dbt_project.dim_indicator": [
        AssetKey(["silver", "stg_indicators"]),
    ],
    "model.dbt_project.fact_economic_indicator": [
        AssetKey(["fct_indicator_value_snapshot"]),
    ],
}

class BridgeDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        extra_deps = _EXTRA_SILVER_DEPS.get(unique_id)
        if extra_deps:
            spec = spec.merge_attributes(deps=extra_deps)
        return spec

@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude="dim_country dim_indicator fact_economic_indicator",
    name="postgres_dbt_assets",
    )
def postgres_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--target", "dev"], context=context).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="dim_country dim_indicator fact_economic_indicator",
    name="clickhouse_dbt_assets",
    dagster_dbt_translator=BridgeDbtTranslator(),
    )
def clickhouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--target", "gold_clickhouse"], context=context).stream()


# WDI Ingestion
sys.path.insert(0, str(Path(__file__).parent.joinpath("../../ingestion").resolve()))
from wdi_client import run as run_wdi_ingestion # noqa: E402

@multi_asset(
    outs={
        "wdi_country_raw": AssetOut(key=AssetKey(["wdi_country_raw"])),
        "wdi_wdi_raw": AssetOut(key=AssetKey(["wdi_wdi_raw"])),
        "wdi_indicator_raw": AssetOut(key=AssetKey(["wdi_indicator_raw"])),
    },
    group_name="ingestion",
    can_subset=False,
    )
def wdi_bronze(context: AssetExecutionContext):
    """
    Pulls WDI indicator, country, and indicator-metadata data and lands it in bronze.bronze.wdi_raw, bronze.country_raw, bronze.indicator_raw — one
    ingestion run, three physical outputs.
    """
    run_wdi_ingestion()

    common_note = MetadataValue.text("World Bank WDI API — full-replace landing")

    yield MaterializeResult(asset_key="wdi_country_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="wdi_wdi_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="wdi_indicator_raw", metadata={"source": common_note})