from dagster import (
    MAX_RUNTIME_SECONDS_TAG,
    ScheduleDefinition,
    RetryPolicy,
    Backoff,
    define_asset_job,
)

# makes job retry with linear backoff. With these settings, the last retry is
# after 7500 seconds (125 minutes)
retry_policy = RetryPolicy(max_retries=5, delay=500, backoff=Backoff.LINEAR)


# build a job that materializes all flood assets
# should timeout after 1 hour
all_flood_assets_job = define_asset_job(
    "all_flood_assets_job",
    selection="flood/raw_discharge*",
    tags={MAX_RUNTIME_SECONDS_TAG: 43000},
    op_retry_policy=retry_policy,
)

# define a schedule that runs the all_assets_job every day at 09:30 UTC
all_flood_assets_schedule = ScheduleDefinition(
    name="all_flood_assets_schedule",
    cron_schedule="30 9 * * *",
    job=all_flood_assets_job,
    execution_timezone="UTC",
)
