import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

RELATIVE_PATH_TO_DBT_PROJECT = "../../dbt_project"

dbt_project = DbtProject(
    project_dir = Path(__file__).parent.joinpath(RELATIVE_PATH_TO_DBT_PROJECT).resolve(),
)

dbt_project.prepare_if_dev()

@dbt_assets(manifest=dbt_project.manifest_path)
def world_economic_data_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()