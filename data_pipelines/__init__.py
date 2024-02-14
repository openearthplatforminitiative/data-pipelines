from dagster import Definitions

from .assets import (
    deforestation_assets,
    river_basin_assets,
)
from .resources import RESOURCES

all_assets = [
    *deforestation_assets,
    *river_basin_assets,
]

defs = Definitions(assets=all_assets, resources=RESOURCES)
