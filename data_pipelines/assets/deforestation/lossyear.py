import os
import rasterio
from rasterio.io import DatasetReader
import xarray as xr
import dask_geopandas
import geopandas as gpd
import dask.array as da
from dask.distributed import Client
from dagster import (
    asset,
    SourceAsset,
    AssetKey,
    Output,
    AssetExecutionContext,
)

from data_pipelines.resources.gfc_io_manager import (
    COGIOManager,
    GeoTIFFIOManager,
    ZarrIOManager,
)
from data_pipelines.partitions import gfc_area_partitions

GFC_BASE_URL = (
    "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10"
)


# @asset(
#     io_manager_def=COGIOManager(base_path="./data"),
#     partitions_def=gfc_area_partitions,
#     key_prefix=["deforestation"],
# )
# def lossyear(context: AssetExecutionContext) -> Output[DatasetReader]:
#     area = context.asset_partition_key_for_output()
#     url = f"{GFC_BASE_URL}/Hansen_GFC-2022-v1.10_lossyear_{area}.tif"
#     dataset = rasterio.open(url)
#     return Output(dataset)


@asset(
    io_manager_def=GeoTIFFIOManager(base_path="./data"),
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
)
def lossyear(context: AssetExecutionContext) -> Output[str]:
    area = context.asset_partition_key_for_output()
    url = f"{GFC_BASE_URL}/Hansen_GFC-2022-v1.10_lossyear_{area}.tif"
    return Output(url)


@asset(
    io_manager_def=ZarrIOManager(base_path="./data"),
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    compute_kind="dask",
)
def treeloss_per_year(lossyear: xr.DataArray) -> Output[xr.DataArray]:
    client = Client()
    print(client.dashboard_link)

    chunk_size = 3 * 1024

    years = list(range(2001, 2023))
    new_shape = (len(years), *lossyear.shape)
    treecover_by_year = xr.DataArray(
        da.zeros(
            new_shape,
            dtype=lossyear.dtype,
            chunks=(1, chunk_size, chunk_size),
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


@asset(
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    compute_kind="dask",
)
def lossyear_points(context: AssetExecutionContext, lossyear: xr.DataArray) -> None:
    client = Client()
    print(client.dashboard_link)
    lossyear = lossyear.chunk({"y": 100, "x": 40_000})
    df = (
        lossyear.drop_vars(["band", "spatial_ref"])
        .rename("lossyear")
        .to_dask_dataframe(dim_order=["y", "x"])
    )
    df = df.loc[df["lossyear"] > 0]

    out_path = os.path.join(
        "./data",
        *context.asset_key_for_output().path,
        f"{context.asset_partition_key_for_output()}.parquet",
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.repartition(partition_size="100MB").to_parquet(
        out_path, write_index=False, overwrite=True
    )
    client.close()


basins = SourceAsset(key=AssetKey(["basins", "basins"]))


@asset(
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    deps=[basins, lossyear_points],
)
def treeloss_per_basin(context: AssetExecutionContext):
    area = context.asset_partition_key_for_output()
    lossyear_points: dask_geopandas.GeoDataFrame = dask_geopandas.read_parquet(
        f"/home/aleks/projects/OpenEPI/data-pipelines/data/deforestation/lossyear_points/{area}.parquet"
    )
    basins = gpd.read_file(
        "/home/aleks/projects/OpenEPI/data-pipelines/data/basins/hybas_af_lev08_v1c.shp"
    )
    lossyear_per_basin_df = basins.sjoin(lossyear_points)
