from dagster import Definitions

from .assets import ALL_ASSETS
from .jobs import glofas_daily_schedule
from .resources import RESOURCES

defs = Definitions(
    assets=ALL_ASSETS, resources=RESOURCES, schedules=[glofas_daily_schedule]
)
