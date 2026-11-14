from dagster import ScheduleDefinition

from .jobs import wdi_refresh_job, comtrade_refresh_job

# WDI revises data a few times a year, not daily (see the indicator-value-snapshot
# discussion) — weekly is a more sensible default cadence than daily for this pipeline.
# Trivially adjustable; the cron string is the only thing that would need to change.
wdi_weekly_schedule = ScheduleDefinition(
    job=wdi_refresh_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Africa/Nairobi",
)

comtrade_weekly_schedule = ScheduleDefinition(
    job=comtrade_refresh_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Africa/Nairobi",
)
