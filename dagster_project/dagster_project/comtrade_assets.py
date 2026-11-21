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
    "model.dbt_project.dim_trade_area": [
        AssetKey(["staging", "stg_comtrade_areas"]),
    ],
    "model.dbt_project.fct_bilateral_trade": [
        AssetKey(["core", "int_comtrade_bilateral_flows"]),
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
        "stg_comtrade_areas stg_comtrade_trade "
        "int_comtrade_bilateral_flows"
    ),
    name="comtrade_postgres_dbt_assets",
)
def comtrade_postgres_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):

    yield from dbt.cli(
        ["build", "--target", "dev"],
        context=context
    ).stream()

# Build tables and views in ClickHouse


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="dim_trade_area fct_bilateral_trade bridge_area_country",
    name="comtrade_clickhouse_dbt_assets",
    dagster_dbt_translator=BridgeDbtTranslator(),
)
def comtrade_clickhouse_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):

    yield from dbt.cli(
        ["build", "--target", "clickhouse"],
        context=context
    ).stream()


# Availing the ingestion code for importing.
sys.path.insert(
    0, str(Path(__file__).parent.joinpath("../../ingestion").resolve()))

from comtrade.pipeline import run as run_comtrade_ingestion  # noqa: E402

# Refresh the raw tables together as one Dagster operation.


@multi_asset(
    outs={
        "comtrade_trade_raw": AssetOut(key=AssetKey(["comtrade_trade_raw"])),
        "comtrade_reporters_raw": AssetOut(key=AssetKey(["comtrade_reporters_raw"])),
        "comtrade_partner_areas_raw": AssetOut(key=AssetKey(["comtrade_partner_areas_raw"])),
    },
    group_name="comtrade",
    can_subset=False,
)
def comtrade_raw_tables(context: AssetExecutionContext):
    """
    Fetch UN Comtrade annual trade data and reference data into PostgreSQL
    raw.comtrade_raw, raw.comtrade_reporters_raw, and
    raw.comtrade_partner_areas_raw. Replace reference tables in full
    and trade rows per query side and year.
    """
    # Fetch UN Comtrade data and save it to PostgreSQL.
    run_comtrade_ingestion()

    common_note = MetadataValue.text(
        "UN Comtrade API — reference tables replaced in full; trade rows replaced per query side and year")

    # Tell Dagster that each raw table has been updated.
    yield MaterializeResult(asset_key="comtrade_trade_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="comtrade_reporters_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="comtrade_partner_areas_raw", metadata={"source": common_note})
