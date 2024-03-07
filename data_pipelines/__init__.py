from dagster import Definitions

from .assets import deforestation_assets, flood_assets, river_basin_assets
from .jobs import glofas_daily_schedule
from .resources import RESOURCES

all_assets = [*deforestation_assets, *river_basin_assets, *flood_assets]

defs = Definitions(
    assets=all_assets, resources=RESOURCES, schedules=[glofas_daily_schedule]
)
