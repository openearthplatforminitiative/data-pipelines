from dagster import Definitions

from .assets import ALL_ASSETS
from .jobs import (
    downstream_asset_sensor,
    downstream_assets_job,
    raw_discharge_daily_schedule,
    raw_discharge_job,
)
from .resources import RESOURCES

defs = Definitions(
    assets=ALL_ASSETS,
    resources=RESOURCES,
    schedules=[raw_discharge_daily_schedule],
    sensors=[downstream_asset_sensor],
    jobs=[raw_discharge_job, downstream_assets_job],
)
