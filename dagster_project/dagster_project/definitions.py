from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_project import wdi_assets, comtrade_assets  # noqa: TID252

from .wdi_assets import dbt_project
from .schedules import wdi_weekly_schedule, comtrade_weekly_schedule

from .jobs import wdi_refresh_job, comtrade_refresh_job

all_assets = load_assets_from_modules([
    wdi_assets,
    comtrade_assets
])

defs = Definitions(
    assets=all_assets,
    jobs=[wdi_refresh_job, comtrade_refresh_job],
    schedules=[wdi_weekly_schedule, comtrade_weekly_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
