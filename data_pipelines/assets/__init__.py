from dagster import load_assets_from_package_module

from . import basin, deforestation, flood

DEFORESTATION = "deforestation"
BASIN = "basin"
FLOOD = "flood"

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name=DEFORESTATION
)
basin_assets = load_assets_from_package_module(package_module=basin, group_name=BASIN)

flood_assets = load_assets_from_package_module(package_module=flood, group_name=FLOOD)

ALL_ASSETS = [*deforestation_assets, *basin_assets, *flood_assets]
