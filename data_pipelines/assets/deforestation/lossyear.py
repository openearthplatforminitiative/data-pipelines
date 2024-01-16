import rasterio
from rasterio.io import DatasetReader
import xarray as xr
import dask.array as da
from dask.distributed import Client
from dagster import (
    asset,
    Output,
    AssetExecutionContext,
)

from data_pipelines.resources.gfc_io_manager import GFCTileIOManager, ZarrIOManager
from data_pipelines.partitions import gfc_area_partitions

GFC_BASE_URL = (
    "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10"
)

CHUNK_SIZE = 3 * 1024


@asset(
    io_manager_def=GFCTileIOManager(base_path="./data", chunk_size=CHUNK_SIZE),
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
)
def lossyear(context: AssetExecutionContext) -> Output[DatasetReader]:
    area = context.asset_partition_key_for_output()
    url = f"{GFC_BASE_URL}/Hansen_GFC-2022-v1.10_lossyear_{area}.tif"
    dataset = rasterio.open(url)
    return Output(dataset)


@asset(
    io_manager_def=ZarrIOManager(base_path="./data", chunk_size=CHUNK_SIZE),
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
)
def treeloss_per_year(lossyear: xr.DataArray) -> Output[xr.DataArray]:
    client = Client()
    print(client)
    years = list(range(2001, 2023))
    new_shape = (len(years), *lossyear.shape)
    treecover_by_year = xr.DataArray(
        da.zeros(
            new_shape,
            dtype=lossyear.dtype,
            chunks=(1, CHUNK_SIZE, CHUNK_SIZE),
        ),
        dims=("year", *lossyear.dims),
        coords={"year": years, **lossyear.coords},
        name="treecover_per_year",
    )

    for year in range(1, 23):
        year_value = 2000 + year
        mask = lossyear == year
        treecover_by_year.loc[year_value] = mask

    return Output(treecover_by_year)
