import os
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, AssetOut, MaterializeResult, MetadataValue, multi_asset
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

RELATIVE_PATH_TO_DBT_PROJECT = "../../dbt_project"

dbt_project = DbtProject(
    project_dir = Path(__file__).parent.joinpath(RELATIVE_PATH_TO_DBT_PROJECT).resolve(),
)

dbt_project.prepare_if_dev()

@dbt_assets(manifest=dbt_project.manifest_path)
def world_economic_data_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

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