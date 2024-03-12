from tempfile import NamedTemporaryFile
from urllib.request import urlretrieve

import dask.dataframe as dd
import geopandas as gpd
import xarray as xr
from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    SourceAsset,
    asset,
)
from flox.xarray import xarray_reduce
from geocube.api.core import make_geocube
from rio_cogeo import cog_profiles, cog_translate

from data_pipelines.partitions import gfc_area_partitions
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.io_managers import get_path_in_asset
from data_pipelines.settings import settings

GLOBAL_FOREST_WATCH_URL_TEMPLATE = (
    "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2022-v1.10/"
    "Hansen_GFC-2022-v1.10_{product}_{area}.tif"
)


@asset(
    io_manager_key="cog_io_manager",
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
)
def lossyear(context: AssetExecutionContext) -> None:
    url = GLOBAL_FOREST_WATCH_URL_TEMPLATE.format(
        product="lossyear", area=context.partition_key
    )

    path = get_path_in_asset(context, settings.base_data_upath, ".tif")
    path.mkdir(parents=True, exist_ok=True)

    context.log.debug("Reading GeoTIFF from %s", url)
    with NamedTemporaryFile() as tmp_file:
        urlretrieve(url, tmp_file.name)
        cog_translate(tmp_file.name, tmp_file.name, cog_profiles["deflate"])
        context.log.debug("Writing COG to %s", path)
        with path.open("wb") as out_file:
            out_file.write(tmp_file.read())


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
    lossyear = lossyear.chunk({"y": 4096, "x": 4096})
    year_masks = [
        (lossyear == y).expand_dims({"year": [y + 2000]}) for y in range(1, 23)
    ]
    treecover_by_year = xr.concat(year_masks, dim="year")
    treecover_by_year = treecover_by_year.coarsen(x=200, y=200).sum()
    treecover_by_year = treecover_by_year.chunk({"year": 22, "x": 200, "y": 200})
    return treecover_by_year


@asset(
    ins={
        "lossyear": AssetIn(
            key_prefix="deforestation", partition_mapping=AllPartitionMapping()
        )
    },
    io_manager_key="parquet_io_manager",
    key_prefix=["deforestation"],
    compute_kind="dask",
)
def lossyear_points(
    context: AssetExecutionContext,
    lossyear: dict[str, xr.DataArray],
    dask_resource: DaskResource,
) -> list[dd.DataFrame]:
    out_data = []
    for lossyear_tile in lossyear.values():
        lossyear_tile = lossyear_tile.squeeze(drop=True).chunk({"y": 512, "x": 40_000})
        df = (
            lossyear_tile.drop_vars(["spatial_ref"])
            .rename("lossyear")
            .to_dask_dataframe(dim_order=["y", "x"])
        )
        df = df.loc[df["lossyear"] > 0].repartition(partition_size="100MB")
        out_data.append(df)
    return out_data


def make_geocube_like_dask(
    df: gpd.GeoDataFrame,
    groups: str | None,
    like: xr.DataArray,
    fill: int = 0,
    **kwargs,
) -> xr.DataArray:
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


def get_bbox_from_GFC_area(area: str) -> tuple[int, int, int, int]:
    # Split the area into latitude and longitude parts
    lat_str, lon_str = area.split("_")

    # Extract numerical values and direction indicators
    lon_num, lon_dir = int(lon_str[:-1]), lon_str[-1]
    lat_num, lat_dir = int(lat_str[:-1]), lat_str[-1]

    # Convert to positive or negative degrees based on direction
    lon = lon_num if lon_dir == "E" else -lon_num
    lat = lat_num if lat_dir == "N" else -lat_num

    return lon, lat, lon + 10, lat - 10


basins = SourceAsset(key=AssetKey(["basins", "basins"]))


@asset(
    io_manager_key="parquet_io_manager",
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    deps=[basins],
    compute_kind="dask",
)
def treeloss_per_basin(
    context: AssetExecutionContext,
    lossyear: xr.DataArray,
    dask_resource: DaskResource,
) -> dd.DataFrame:
    lossyear = lossyear.chunk({"y": 200, "x": 40_000}).rename("lossyear")
    bbox = get_bbox_from_GFC_area(context.asset_partition_key_for_input("lossyear"))

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
    return loss_per_basin_df
