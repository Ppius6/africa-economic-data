import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from dagster import AssetExecutionContext, AssetKey, AssetOut, MaterializeResult, MetadataValue, multi_asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

# Load environment variables from the project's .env file.
load_dotenv(Path(__file__).parent.joinpath("../../.env").resolve())

RELATIVE_PATH_TO_DBT_PROJECT = "../../dbt_project"

# Find the dbt project containing the SQL models.
dbt_project = DbtProject(
    project_dir=Path(__file__).parent.joinpath(
        RELATIVE_PATH_TO_DBT_PROJECT).resolve(),
)

# Prepare dbt files when running Dagster locally in development mode.
dbt_project.prepare_if_dev()

# Map ClickHouse models to the PostgreSQL assets they depend on.
_EXTRA_POSTGRES_DEPS = {
    "model.dbt_project.dim_country": [
        AssetKey(["staging", "stg_countries"]),
        AssetKey(["core", "income_classification_history"]),
        AssetKey(["dim_country_snapshot"]),
    ],
    "model.dbt_project.dim_indicator": [
        AssetKey(["staging", "stg_indicators"]),
    ],
    "model.dbt_project.fct_economic_indicator": [
        AssetKey(["fct_indicator_value_snapshot"]),
    ],
}


# Add these dependencies to Dagster's asset graph.
class BridgeDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        extra_deps = _EXTRA_POSTGRES_DEPS.get(unique_id)
        if extra_deps:
            spec = spec.merge_attributes(deps=extra_deps)
        return spec


# Build staging tables and historical data in PostgreSQL.
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=(
        "stg_countries stg_indicators stg_wdi_indicators "
        "dim_country_snapshot fct_indicator_value_snapshot "
        "income_classification_history"
    ),
    name="wdi_postgres_dbt_assets",
)
def wdi_postgres_dbt_assets(
        context: AssetExecutionContext,
        dbt: DbtCliResource):

    yield from dbt.cli(
        ["build", "--target", "dev"],
        context=context
    ).stream()


# Build dimension and fact tables in ClickHouse.
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="dim_country dim_indicator fct_economic_indicator",
    name="wdi_clickhouse_dbt_assets",
    dagster_dbt_translator=BridgeDbtTranslator(),
)
def wdi_clickhouse_dbt_assets(
        context: AssetExecutionContext,
        dbt: DbtCliResource):

    yield from dbt.cli(
        ["build", "--target", "clickhouse"],
        context=context
    ).stream()


# Make the ingestion code available to import.
sys.path.insert(
    0, str(Path(__file__).parent.joinpath("../../ingestion").resolve()))
from wdi.pipeline import run as run_wdi_ingestion  # noqa: E402


# Refresh all three raw tables together as one Dagster operation.
@multi_asset(
    outs={
        "wdi_country_raw": AssetOut(key=AssetKey(["wdi_country_raw"])),
        "wdi_wdi_raw": AssetOut(key=AssetKey(["wdi_wdi_raw"])),
        "wdi_indicator_raw": AssetOut(key=AssetKey(["wdi_indicator_raw"])),
    },
    group_name="wdi",
    can_subset=False,
)
def wdi_raw_tables(context: AssetExecutionContext):
    """
    Fetch World Bank WDI observations and metadata into PostgreSQL
    raw.wdi_raw, raw.country_raw, and raw.indicator_raw.
    Replace metadata tables in full and observations per indicator.
    """
    # Fetch World Bank data and save it to PostgreSQL.
    run_wdi_ingestion()

    common_note = MetadataValue.text(
        "World Bank WDI API — metadata replaced in full; observations replaced per indicator")

    # Tell Dagster that each raw table has been updated.
    yield MaterializeResult(asset_key="wdi_country_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="wdi_wdi_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="wdi_indicator_raw", metadata={"source": common_note})
