import io
import zipfile

import geopandas as gpd
import httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    SourceAsset,
    asset,
)

from data_pipelines.partitions import gfc_area_partitions
from data_pipelines.settings import settings

HYDROSHEDS_URL = (
    "https://data.hydrosheds.org/file/hydrobasins/standard/hybas_af_lev01-12_v1c.zip"
)
HYDROSHEDS_BASIN_LEVEL = 7


@asset(key_prefix="basin")
def hydrobasins(context: AssetExecutionContext) -> MaterializeResult:
    r = httpx.get(HYDROSHEDS_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    file_name = f"hybas_af_lev{HYDROSHEDS_BASIN_LEVEL:02}_v1c"
    asset_name = "hydrobasins"
    for extension in [".dbf", ".prj", ".sbn", ".sbx", ".shp", ".shp.xml", ".shx"]:
        out_path = settings.base_data_upath.joinpath(
            "basin", "hydrobasins", asset_name
        ).with_suffix(extension)
        out_path.write_bytes(z.read(file_name + extension))
    return MaterializeResult(asset_key=AssetKey(["basin", asset_name]))


def parse_coordinates(coord_list):
    lat_min, lat_max, lon_min, lon_max = (
        float("inf"),
        float("-inf"),
        float("inf"),
        float("-inf"),
    )

    for coord in coord_list:
        # Parse the coordinates from the tile identifier
        lat_str, lon_str = coord.split("_")

        # Extract numbers and directions
        lon_num, lon_dir = int(lon_str[:-1]), lon_str[-1]
        lat_num, lat_dir = int(lat_str[:-1]), lat_str[-1]

        # Convert to geographical coordinates
        lon = lon_num if lon_dir == "E" else -lon_num
        lat = lat_num if lat_dir == "N" else -lat_num

        # Calculate the geographic extents of the tile
        lon_min_tile = lon
        lon_max_tile = lon + 10 if lon_dir == "E" else lon - 10
        lat_min_tile = lat - 10 if lat_dir == "N" else lat + 10
        lat_max_tile = lat

        # Update the global min/max with the tile's min/max
        lon_min = min(lon_min, lon_min_tile)
        lon_max = max(lon_max, lon_max_tile)
        lat_min = min(lat_min, lat_min_tile)
        lat_max = max(lat_max, lat_max_tile)

    return lon_min, lat_min, lon_max, lat_max


@asset(key_prefix="basin", deps=[SourceAsset(key=AssetKey(["basin", "hydrobasins"]))])
def basins(context: AssetExecutionContext) -> MaterializeResult:
    basin_path = settings.base_data_upath.joinpath(
        "basin", "hydrobasins", "hydrobasins.shp"
    )
    bbox = parse_coordinates(gfc_area_partitions.get_partition_keys())
    context.log.info(f"Reading basins with bounding box: {bbox}")
    basins = gpd.read_file(basin_path.as_uri(), bbox=bbox)

    basins: gpd.GeoDataFrame = basins.rename(
        columns={
            "HYBAS_ID": "id",
            "NEXT_DOWN": "downstream",
            "SUB_AREA": "basin_area",
            "UP_AREA": "upstream_area",
        }
    )

    basins_output_path = settings.base_data_upath.joinpath(
        "basin", "basins"
    ).with_suffix(".parquet")

    basins[["id", "downstream", "basin_area", "upstream_area", "geometry"]].to_parquet(
        basins_output_path.as_uri(),
        storage_options=basins_output_path.storage_options,
        index=False,
    )

    return MaterializeResult(asset_key=AssetKey(["basin", "basins"]))
