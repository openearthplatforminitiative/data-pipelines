from dagster import Definitions

from .assets import ALL_ASSETS
from .jobs import (
    all_flood_assets_schedule,
    all_flood_assets_job
)
from .resources import RESOURCES

defs = Definitions(
    assets=ALL_ASSETS,
    resources=RESOURCES,
    schedules=[all_flood_assets_schedule],
    jobs=[all_flood_assets_job],
)
