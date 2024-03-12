from dagster import load_assets_from_package_module

from . import basins, deforestation, flood

DEFORESTATION = "deforestation"
BASINS = "basin"
FLOOD = "flood"

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name=DEFORESTATION
)
basin_assets = load_assets_from_package_module(package_module=basins, group_name=BASINS)

flood_assets = load_assets_from_package_module(package_module=flood, group_name=FLOOD)

ALL_ASSETS = [*deforestation_assets, *basin_assets, *flood_assets]
