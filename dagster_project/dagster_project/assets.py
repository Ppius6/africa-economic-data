import os
import sys
from pathlib import Path

from dagster import AssetExecutionContext, asset, MaterializeResult, MetadataValue
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
from wdi_client import run as run_wdi_ingestion

@asset(group_name="ingestion")
def wdi_bronze(context: AssetExecutionContext) -> MaterializeResult:
    """
    Pulls WDI indicator, country, and indicator-metadata data and lands it in bronze.
    """
    run_wdi_ingestion()
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text("World Bank WDI API"),
            "note": MetadataValue.text(
                "Full-replace landing into bronze.wdi_raw, bronze.country_raw, bronze.indicator_raw"
            ),
        }
    )