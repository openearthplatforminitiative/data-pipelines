import rasterio
from rasterio.io import DatasetReader
from dagster import (
    asset,
    Output,
    AssetExecutionContext,
)

from data_pipelines.partitions import gfc_area_partitions
from data_pipelines.resources.gfc_io_manager import COGIOManager

GFC_BASE_URL = (
    "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10"
)


@asset(
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    io_manager_def=COGIOManager(base_path="./data"),
)
def treecover2000(context: AssetExecutionContext) -> Output[DatasetReader]:
    area = context.asset_partition_key_for_output()
    url = f"{GFC_BASE_URL}/Hansen_GFC-2022-v1.10_treecover2000_{area}.tif"
    dataset = rasterio.open(url)
    return Output(dataset)
