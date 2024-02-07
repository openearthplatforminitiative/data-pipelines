import os

from dagster_pyspark import PySparkResource
from dotenv import load_dotenv

from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.utils.flood.config import GLOFAS_API_URL

from .dask_resource import DaskResource
from .io_managers import COGIOManager, GeoTIFFIOManager, ParquetIOManager, ZarrIOManager

load_dotenv()

# Define API access variables using environment variables
user_id = os.environ["CDS_USER_ID"]
api_key = os.environ["CDS_API_KEY"]

# glofas_pyspark = PySparkResource(spark_config={"spark.executor.memory": "2g"})
glofas_pyspark = PySparkResource(spark_config={"spark.default.parallelism": 1})


RESOURCES = {
    "dask_resource": DaskResource(),
    "geotiff_io_manager": GeoTIFFIOManager(),
    "cog_io_manager": COGIOManager(),
    "zarr_io_manager": ZarrIOManager(),
    "parquet_io_manager": ParquetIOManager(),
    "client": CDSClient(api_url=GLOFAS_API_URL, api_key=f"{user_id}:{api_key}"),
    "pyspark": glofas_pyspark,
}
