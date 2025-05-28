from dagster import asset, AssetExecutionContext
from data_pipelines.resources.copernicus_resource import CopernicusClient
from data_pipelines.assets.sentinel.config import IngestConfig
from shapely.geometry import shape
import shapefile
import os

datapath = os.getenv("SENTINEL_DATA_DIR")


@asset(key_prefix=["sentinel"], io_manager_key="json_io_manager")
def raw_imagery(
    context: AssetExecutionContext,
    config: IngestConfig,
    copernicus_client: CopernicusClient,
) -> dict:
    shp = shapefile.Reader(f"{datapath}/{config.area_shp_path}")
    geo = shp.shape().__geo_interface__
    area = shape(geo)

    context.log.info(f"Starting download for {config.year}/{config.quartile}")
    product_info = copernicus_client.findProducts(area, config.year, config.quartile)
    context.log.info(f"Finished download for {config.year}/{config.quartile}")

    return product_info
