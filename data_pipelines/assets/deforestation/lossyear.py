import os

from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    AssetIn,
    SourceAsset,
)
import dask.dataframe as dd
from flox.xarray import xarray_reduce
from geocube.api.core import make_geocube
import geopandas as gpd
import rioxarray
from rio_cogeo import cog_info
import xarray as xr

from data_pipelines.partitions import gfc_area_partitions
from data_pipelines.resources.dask_resource import DaskResource

GFC_BASE_URL = (
    "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10"
)

DATA_BASE_PATH = "/home/aleks/projects/OpenEPI/data-pipelines/data"


@asset(
    io_manager_key="geotiff_io_manager",
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
)
def lossyear(context: AssetExecutionContext) -> str:
    area = context.asset_partition_key_for_output()
    url = f"{GFC_BASE_URL}/Hansen_GFC-2022-v1.10_lossyear_{area}.tif"
    return url


@asset(
    io_manager_key="zarr_io_manager",
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    compute_kind="dask",
)
def treeloss_per_year(
    lossyear: xr.DataArray,
    dask_resource: DaskResource,
) -> xr.DataArray:
    lossyear = lossyear.chunk({"y": 2000, "x": 40_000})

    year_masks = [
        (lossyear == y).expand_dims({"year": [y + 2000]}) for y in range(1, 23)
    ]
    treecover_by_year = xr.concat(year_masks, dim="year")
    treecover_by_year = treecover_by_year.coarsen(x=200, y=200).sum()
    treecover_by_year = treecover_by_year.chunk({"year": 22, "x": 200, "y": 200})
    return treecover_by_year


@asset(
    io_manager_key="parquet_io_manager",
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    compute_kind="dask",
)
def lossyear_points(
    context: AssetExecutionContext,
    lossyear: xr.DataArray,
    dask_resource: DaskResource,
) -> dd.DataFrame:
    lossyear = lossyear.chunk({"y": 200, "x": 40_000})
    df = (
        lossyear.drop_vars(["band", "spatial_ref"])
        .rename("lossyear")
        .to_dask_dataframe(dim_order=["y", "x"])
    )
    df = df.loc[df["lossyear"] > 0]
    return df.repartition(partition_size="100MB")


def make_geocube_like_dask(
    df: gpd.GeoDataFrame,
    groups: str | None,
    like: xr.DataArray,
    fill: int = 0,
    **kwargs,
) -> xr.DataArray:
    """This function is based on a solution by user jessjaco in the geocube repo:
    https://github.com/corteva/geocube/issues/41#issuecomment-885911090
    """

    def rasterize_block(block):
        return (
            make_geocube(df, measurements=[groups], like=block, fill=fill, **kwargs)
            .to_array(groups)
            .assign_coords(block.coords)
        )

    like = like.rename({"band": groups})
    return (
        like.map_blocks(rasterize_block, template=like)
        .rename(groups)
        .squeeze(drop=True)
    )


basins = SourceAsset(key=AssetKey(["basins", "basins"]))


@asset(
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    deps=[basins],
    compute_kind="dask",
)
def treeloss_per_basin(
    context: AssetExecutionContext,
    lossyear: xr.DataArray,
    dask_resource: DaskResource,
):
    area = context.asset_partition_key_for_output()
    # Open lossyear data
    lossyear_path = os.path.join(
        DATA_BASE_PATH, "deforestation", "lossyear", f"{area}.tif"
    )
    lossyear = rioxarray.open_rasterio(
        lossyear_path, chunks={"y": 200, "x": 40_000}
    ).rename("lossyear")
    bbox = cog_info(lossyear_path).GEO.BoundingBox

    # Open basin data
    basins = gpd.read_file(
        "/home/aleks/projects/OpenEPI/data-pipelines/data/basins/hybas_af_lev08_v1c.shp",
        bbox=bbox,
    )

    # Rasterize basin data to lossyear grid and combine into dataset
    basin_zones = make_geocube_like_dask(basins, "HYBAS_ID", lossyear).to_dataset()
    basin_zones["year"] = lossyear.where(lossyear > 0).squeeze(drop=True)
    basin_zones["tree_loss_incidents"] = xr.ones_like(basin_zones["year"])

    # Calculate number of deforestation events per basin and year
    loss_per_basin = xarray_reduce(
        basin_zones.tree_loss_incidents,
        basin_zones.HYBAS_ID,
        basin_zones.year,
        func="count",
        expected_groups=(basins.HYBAS_ID.unique(), list(range(1, 23))),
    )
    # The resulting dataframe has 3 columns: "HYBAS_ID", "year" and "tree_loss_incidents"
    loss_per_basin_df = loss_per_basin.drop_vars("spatial_ref").to_dask_dataframe()

    # Write to parquet
    out_path = os.path.join(
        DATA_BASE_PATH, "deforestation", "treeloss_per_basin", f"{area}.nc"
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    loss_per_basin_df.to_parquet(out_path, write_index=False, overwrite=True)
