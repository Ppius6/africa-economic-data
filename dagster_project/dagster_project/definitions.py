from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_project import assets  # noqa: TID252

from .assets import dbt_project, world_economic_data_dbt_assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir),
    },
)
