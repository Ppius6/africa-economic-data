from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_project import assets  # noqa: TID252

from .assets import dbt_project
from .schedules import weekly_schedule

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=[weekly_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
