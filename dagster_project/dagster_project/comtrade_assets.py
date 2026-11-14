import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from dagster import AssetExecutionContext, AssetKey, AssetOut, MaterializeResult, MetadataValue, multi_asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

load_dotenv(Path(__file__).parent.joinpath("../../.env").resolve())

RELATIVE_PATH_TO_DBT_PROJECT = "../../dbt_project"

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.joinpath(
        RELATIVE_PATH_TO_DBT_PROJECT).resolve(),
)

dbt_project.prepare_if_dev()

_EXTRA_SILVER_DEPS = {
    "model.dbt_project.dim_trade_area": [
        AssetKey(["silver", "stg_comtrade_areas"]),
    ],
    "model.dbt_project.fct_bilateral_trade": [
        AssetKey(["silver", "int_comtrade_bilateral_flows"]),
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
            ["build", "--target", "gold_clickhouse"], 
            context=context
        ).stream()


# Comtrade Ingestion
sys.path.insert(
    0, str(Path(__file__).parent.joinpath("../../ingestion").resolve()))

from comtrade.pipeline import run as run_comtrade_ingestion  # noqa: E402

@multi_asset(
    outs={
        "comtrade_trade_raw": AssetOut(key=AssetKey(["comtrade_trade_raw"])),
        "comtrade_reporters_raw": AssetOut(key=AssetKey(["comtrade_reporters_raw"])),
        "comtrade_partner_areas_raw": AssetOut(key=AssetKey(["comtrade_partner_areas_raw"])),
    },
    group_name="comtrade",
    can_subset=False,
)
def comtrade_bronze(context: AssetExecutionContext):
    """
    Fetch UN Comtrade annual trade data and reference data into PostgreSQL
    bronze.comtrade_raw, bronze.comtrade_reporters_raw, and
    bronze.comtrade_partner_areas_raw. Replace reference tables in full
    and trade rows per query side and year.
    """
    run_comtrade_ingestion()

    common_note = MetadataValue.text(
        "UN Comtrade API — reference tables replaced in full; trade rows replaced per query side and year")

    yield MaterializeResult(asset_key="comtrade_trade_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="comtrade_reporters_raw", metadata={"source": common_note})
    yield MaterializeResult(asset_key="comtrade_partner_areas_raw", metadata={"source": common_note})
