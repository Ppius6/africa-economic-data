from dagster import AssetSelection, define_asset_job

# One job covering the whole pipeline: ingestion -> Postgres (bronze/silver) -> ClickHouse
# (gold). Dagster resolves execution order from the dependency graph itself (verified
# correct across every layer boundary this session), so selecting "everything" is safe —
# no manual sequencing needed here.
full_refresh_job = define_asset_job("full_refresh", selection=AssetSelection.all())
