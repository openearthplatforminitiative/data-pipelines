from dagster import asset, AssetExecutionContext
from data_pipelines.resources.copernicus_resource import CopernicusClient
from data_pipelines.assets.sentinel.config import IngestConfig
from shapely import Polygon
from shapely.geometry import shape
import shapefile


@asset(key_prefix=["sentinel"], io_manager_key="json_io_manager")
def raw_imagery(context: AssetExecutionContext, config: IngestConfig, copernicus_client: CopernicusClient) -> dict:
    shp = shapefile.Reader(config.area_shp_path)
    geo = shp.shape().__geo_interface__
    area = shape(geo)

    product_info = copernicus_client.findProducts(area, config.year, config.quartile)

    return product_info
