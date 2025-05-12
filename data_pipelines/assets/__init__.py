from dagster import load_assets_from_package_module

from . import basin, deforestation, flood, sentinel

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name="deforestation"
)
basin_assets = load_assets_from_package_module(package_module=basin, group_name="basin")

flood_assets = load_assets_from_package_module(package_module=flood, group_name="flood")

sentinel_assets = load_assets_from_package_module(
    package_module=sentinel, group_name="sentinel"
)

ALL_ASSETS = [*deforestation_assets, *basin_assets, *flood_assets, *sentinel_assets]
