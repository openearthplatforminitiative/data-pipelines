from dagster import load_assets_from_package_module

from . import basins, deforestation

DEFORESTATION = "deforestation"
BASINS = "basins"

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name=DEFORESTATION
)
river_basin_assets = load_assets_from_package_module(
    package_module=basins, group_name=BASINS
)
