import os

from dagster import Definitions
from dotenv import load_dotenv

from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.utils.flood.config import GLOFAS_API_URL

from .assets import deforestation_assets, discharge_assets, river_basin_assets
from .resources import RESOURCES

all_assets = [*deforestation_assets, *river_basin_assets, *discharge_assets]

defs = Definitions(assets=all_assets, resources=RESOURCES)
