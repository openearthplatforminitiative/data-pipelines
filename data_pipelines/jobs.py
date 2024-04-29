from dagster import build_schedule_from_partitioned_job, define_asset_job

from data_pipelines.partitions import discharge_partitions

# Define the job that targets the partitioned asset
glofas_daily_update_job = define_asset_job(
    "glofas_daily_update",
    selection="flood/raw_discharge*",
    partitions_def=discharge_partitions,
    tags={"sequential_backfill": "true"},
)

# Create a schedule for the job using build_schedule_from_partitioned_job
glofas_daily_schedule = build_schedule_from_partitioned_job(
    job=glofas_daily_update_job,
    cron_schedule="30 11 * * *",
    execution_timezone="UTC",
)
