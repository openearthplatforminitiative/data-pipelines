from dagster import ScheduleDefinition, define_asset_job

# Materializes flood/raw_discharge and all downstream assets every day at 11:30
glofas_daily_schedule = ScheduleDefinition(
    job=define_asset_job("glofas_daily_update", selection="flood/raw_discharge*"),
    cron_schedule="30 11 * * *",
)
