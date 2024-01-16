import rasterio
from rasterio.io import DatasetReader
from dagster import (
    asset,
    Output,
    AssetExecutionContext,
)

from data_pipelines.partitions import gfc_area_partitions
from data_pipelines.resources.gfc_io_manager import GFCTileIOManager

GFC_BASE_URL = (
    "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10"
)

CHUNK_SIZE = 3 * 1024


@asset(
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    io_manager_def=GFCTileIOManager(base_path="./data", chunk_size=CHUNK_SIZE),
)
def treecover2000(context: AssetExecutionContext) -> Output[DatasetReader]:
    area = context.asset_partition_key_for_output()
    url = f"{GFC_BASE_URL}/Hansen_GFC-2022-v1.10_treecover2000_{area}.tif"
    dataset = rasterio.open(url)
    return Output(dataset)
