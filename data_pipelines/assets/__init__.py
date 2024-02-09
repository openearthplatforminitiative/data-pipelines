from dagster import load_assets_from_package_module

from . import basins, deforestation, flood

DEFORESTATION = "deforestation"
BASINS = "basins"
FLOOD = "flood"

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name=DEFORESTATION
)
river_basin_assets = load_assets_from_package_module(
    package_module=basins, group_name=BASINS
)

flood_assets = load_assets_from_package_module(package_module=flood, group_name=FLOOD)
