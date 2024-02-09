import os
from dotenv import load_dotenv

from data_pipelines.utils.flood.config import GLOFAS_API_URL
from .dask_resource import DaskResource
from .io_managers import (
    GeoTIFFIOManager,
    COGIOManager,
    ZarrIOManager,
    ParquetIOManager,
)
from data_pipelines.resources.glofas_resource import CDSClient

load_dotenv()

# Define API access variables using environment variables
user_id = os.environ["CDS_USER_ID"]
api_key = os.environ["CDS_API_KEY"]

RESOURCES = {
    "dask_resource": DaskResource(),
    "geotiff_io_manager": GeoTIFFIOManager(),
    "cog_io_manager": COGIOManager(),
    "zarr_io_manager": ZarrIOManager(),
    "parquet_io_manager": ParquetIOManager(),
    "client": CDSClient(api_url=GLOFAS_API_URL, api_key=f"{user_id}:{api_key}"),
}
