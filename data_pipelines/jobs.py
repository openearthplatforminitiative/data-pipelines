from dagster import ScheduleDefinition, define_asset_job

# build a job that materializes all assets
all_flood_assets_job = define_asset_job(
    "all_assets_job",
    selection="flood/raw_discharge*",
)

# define a schedule that runs the all_assets_job every day at 11:00 UTC
# without using build_schedule_from_partitioned_job
all_flood_assets_schedule = ScheduleDefinition(
    name="all_assets_schedule",
    cron_schedule="44 16 * * *",
    job=all_flood_assets_job,
    execution_timezone="UTC",
)
