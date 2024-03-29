from dagster import load_assets_from_package_module

from . import basin, deforestation, flood

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name="deforestation"
)
basin_assets = load_assets_from_package_module(package_module=basin, group_name="basin")

flood_assets = load_assets_from_package_module(package_module=flood, group_name="flood")

ALL_ASSETS = [*deforestation_assets, *basin_assets, *flood_assets]
