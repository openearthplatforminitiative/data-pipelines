from tempfile import NamedTemporaryFile
from urllib.request import urlretrieve

import dask.array as da
import dask.dataframe as dd
import geopandas as gpd
import rio_cogeo
import xarray as xr
from dagster import AssetExecutionContext, AssetIn, AssetKey, SourceAsset, asset
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


def get_resolution(raster_src: str | xr.DataArray | xr.Dataset) -> float:
    """Return the resoluton of a GeoTIFF given by path or an open xarray object."""
    if isinstance(raster_src, str):
        return abs(rio_cogeo.cog_info(raster_src).GEO.Resolution[0])
    else:
        return abs(raster_src.rio.resolution()[0])


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
    ins={"lossyear": AssetIn(key_prefix="deforestation")},
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


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the haversine distance between two geographical points in meters."""
    R = 6371  # radius of Earth in km
    phi_1 = da.radians(lat1)
    phi_2 = da.radians(lat2)
    delta_phi = da.radians(lat2 - lat1)
    delta_lambda = da.radians(lon2 - lon1)
    a = (
        da.sin(delta_phi / 2.0) ** 2
        + da.cos(phi_1) * da.cos(phi_2) * da.sin(delta_lambda / 2.0) ** 2
    )
    c = 2 * da.arctan2(da.sqrt(a), da.sqrt(1 - a))
    meters = R * c  # output distance in km
    return meters


def calculate_pixel_area(lat, lon, pixel_size):
    """Calculate the pixel area using haversine formula based on latitude and longitude."""
    lat_north = lat + pixel_size / 2
    lat_south = lat - pixel_size / 2
    lon_east = lon + pixel_size / 2
    lon_west = lon - pixel_size / 2

    # Calculate distances using haversine formula
    height = haversine(lat_south, lon, lat_north, lon)
    width = haversine(lat, lon_west, lat, lon_east)

    return height * width


@asset(
    ins={"lossyear": AssetIn(key_prefix="deforestation")},
    io_manager_key="parquet_io_manager",
    partitions_def=gfc_area_partitions,
    key_prefix=["deforestation"],
    deps=[SourceAsset(key=AssetKey(["basin", "hydrobasins"]))],
    compute_kind="dask",
)
def treeloss_per_basin(
    context: AssetExecutionContext,
    lossyear: xr.DataArray,
    dask_resource: DaskResource,
) -> dd.DataFrame:
    lossyear = lossyear.chunk({"y": 4096, "x": 4096}).rename("lossyear")
    # reduce to a smaller region of interest
    # lat_min, lat_max, lon_min, lon_max = 1.0, 0.0, 20.0, 21.0
    # lossyear = lossyear.sel(
    #     y=slice(lat_min, lat_max),
    #     x=slice(lon_min, lon_max),
    # )

    # context.log.info(f"Lossyear: {lossyear}")
    bbox = get_bbox_from_GFC_area(context.asset_partition_key_for_input("lossyear"))

    # Open basin data
    basin_path = settings.base_data_upath.joinpath(
        "basin", "hydrobasins", "hydrobasins.shp"
    )

    basins = gpd.read_file(
        basin_path.as_uri(), bbox=bbox
    )  # (lon_min, lat_min, lon_max, lat_max))

    # Rasterize basin data to lossyear grid and combine into dataset
    basin_zones = make_geocube_like_dask(basins, "HYBAS_ID", lossyear).to_dataset()
    if "HYBAS_ID" in basin_zones:
        basin_zones["HYBAS_ID"] = basin_zones["HYBAS_ID"].astype("int64")

    # print dtypes of basin_zones
    context.log.info(f"Basin zones dtypes: {basin_zones.dtypes}")

    basin_df = basin_zones.drop_vars("spatial_ref").to_dask_dataframe().reset_index()
    # cast HYBAS ID to int
    # basin_zones["HYBAS_ID"] = basin_zones["HYBAS_ID"].astype(int)
    # basin_zones = basin_zones.compute()

    context.log.info(f"Basin zones: {basin_df}")

    # Calculate cell area
    pixel_size = get_resolution(lossyear)
    context.log.info(f"Pixel size: {pixel_size}")

    # group the dataframe by 'HYBAS_ID' and calculate the first cell area
    grouped_areas_df = basin_df.groupby("HYBAS_ID").first()
    context.log.info(f"first cell: {grouped_areas_df}")
    grouped_areas_df["first_cell_area"] = calculate_pixel_area(
        grouped_areas_df["y"], grouped_areas_df["x"], pixel_size
    )
    grouped_areas_df = grouped_areas_df.drop(columns=["x", "y", "index"])

    context.log.info(f"First cell df: {grouped_areas_df}")

    # Assuming basin_zones is your xarray Dataset
    # Convert 'x' and 'y' from coordinates to data variables
    # basin_zones = basin_zones.assign(x=basin_zones.x, y=basin_zones.y)

    # Now perform the groupby operation and select the first 'x' and 'y' for each 'HYBAS_ID'
    # context.log.info(f"Basin zones: {basin_zones}")
    # grouped_data = basin_zones.groupby('HYBAS_ID').first()
    # context.log.info(f"Grouped data: {grouped_data}")
    # first_x = grouped_data['x']
    # first_y = grouped_data['y']

    # def compute_representative_area(basin_zones: xr.DataArray, pixel_size):
    #     # Group by 'HYBAS_ID' and get the first 'x' and 'y' values
    #     grouped_x = basin_zones.groupby(basin_zones['HYBAS_ID']).first()
    #     context.log.info(f"Grouped x: {grouped_x}")
    #     grouped_y = basin_zones.y.groupby(basin_zones['HYBAS_ID']).first()

    #     # Compute the area for these coordinates
    #     areas = calculate_pixel_area(grouped_y, grouped_x, pixel_size)

    #     return areas

    # # Usage:
    # mean_cell_areas = compute_representative_area(basin_zones, pixel_size)

    # # You can convert this to a DataArray and attach it back to your dataset if needed
    # mean_cell_areas_da = xr.DataArray(mean_cell_areas, dims=["HYBAS_ID"])
    # mean_cell_areas_da.name = "mean_cell_area"
    # basin_zones['mean_cell_area'] = basin_zones['HYBAS_ID'].map(mean_cell_areas_da)

    # areas = calculate_pixel_area(first_y, first_x, pixel_size)

    # Convert areas to a DataArray (if necessary) and attach back to the dataset
    # mean_cell_areas = xr.DataArray(areas, dims=["HYBAS_ID"])
    # mean_cell_areas.name = "mean_cell_area"
    # basin_zones['mean_cell_area'] = mean_cell_areas

    # context.log.info(f"Basin zones: {basin_zones}")

    # cell_areas = calculate_pixel_area(basin_zones.y, basin_zones.x, pixel_size)
    # context.log.info(f"Cell areas: {cell_areas}")

    # basin_zones["cell_area"] = (["y", "x"], cell_areas.data)

    # context.log.info(f"Basin zones: {basin_zones}")

    # # Compute the mean of cell areas per basin
    # cell_area_da = basin_zones["cell_area"]
    # grouped_areas = (
    #     cell_area_da.groupby(basin_zones["HYBAS_ID"])
    #     .mean()
    #     .rename("mean_cell_area")
    #     .drop_vars("spatial_ref")
    # )

    # context.log.info(f"Grouped areas: {grouped_areas}")

    # # drop the cell area from the dataset
    # basin_zones = basin_zones.drop_vars("cell_area")

    basin_zones["year"] = lossyear.where(lossyear > 0).squeeze(drop=True)
    basin_zones["tree_loss_incidents"] = xr.ones_like(basin_zones["year"])

    # Calculate number of deforestation events per basin and yea
    loss_per_basin = xarray_reduce(
        basin_zones.tree_loss_incidents,
        basin_zones.HYBAS_ID,
        basin_zones.year,
        func="count",
        expected_groups=(basins.HYBAS_ID.unique(), list(range(1, 23))),
    )

    # add 2000 to the year index
    loss_per_basin["year"] = loss_per_basin["year"] + 2000

    context.log.info(f"Loss per basin: {loss_per_basin}")

    # The resulting dataframe has 3 columns: "HYBAS_ID", "year" and "tree_loss_incidents"
    loss_per_basin_df = loss_per_basin.drop_vars("spatial_ref").to_dask_dataframe()
    context.log.info(f"Loss per basin df: {loss_per_basin_df}")
    aggregated_df = (
        loss_per_basin_df.groupby(["HYBAS_ID", "year"])
        .agg({"tree_loss_incidents": "sum"})
        .reset_index()
    )
    # grouped_areas_df = grouped_areas.to_dask_dataframe()

    context.log.info(f"Loss per basin df: {aggregated_df}")
    context.log.info(f"Grouped areas df: {grouped_areas_df}")

    result_df = aggregated_df.merge(grouped_areas_df, on="HYBAS_ID", how="left")

    context.log.info(f"Result df: {result_df}")

    return result_df
