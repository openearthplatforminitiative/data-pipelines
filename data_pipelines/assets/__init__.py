from dagster import load_assets_from_package_module

from . import basins, deforestation, discharge

DEFORESTATION = "deforestation"
BASINS = "basins"
DISCHARGE = "discharge"

deforestation_assets = load_assets_from_package_module(
    package_module=deforestation, group_name=DEFORESTATION
)
river_basin_assets = load_assets_from_package_module(
    package_module=basins, group_name=BASINS
)
discharge_assets = load_assets_from_package_module(
    package_module=discharge, group_name=DISCHARGE
)
