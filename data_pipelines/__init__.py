import os
from dagster import Definitions

from .assets import (
    deforestation_assets,
    river_basin_assets,
)

all_assets = [
    *deforestation_assets,
    *river_basin_assets,
]

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
)
