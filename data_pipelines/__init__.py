import os
from dagster import Definitions

from .assets import deforestation_assets, river_basin_assets, flood_assets

from .resources import RESOURCES

all_assets = [*deforestation_assets, *river_basin_assets, *flood_assets]

defs = Definitions(assets=all_assets, resources=RESOURCES)
