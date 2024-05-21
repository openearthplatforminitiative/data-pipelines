import os

import httpx
import xarray as xr
from dagster import AssetExecutionContext, AssetIn, asset

from data_pipelines.resources.io_managers import (
    copy_local_file_to_s3,
    get_path_in_asset,
)
from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.config import UPSTREAM_URL
from data_pipelines.utils.flood.utils import restrict_dataset_area


@asset(key_prefix=["flood"], io_manager_key="netcdf_io_manager")
def uparea_glofas_v4_0(context: AssetExecutionContext) -> None:
    out_path = get_path_in_asset(context, settings.base_data_upath, ".nc")
    out_path.mkdir(parents=True, exist_ok=True)

    response = httpx.get(UPSTREAM_URL)
    out_path.write_bytes(response.content)
    return None


@asset(
    key_prefix=["flood"],
    io_manager_key="netcdf_io_manager",
    ins={"uparea_glofas_v4_0": AssetIn(key_prefix="flood")},
)
def restricted_uparea_glofas_v4_0(
    context: AssetExecutionContext, uparea_glofas_v4_0: xr.Dataset
) -> None:
    buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV
    lat_min = GLOFAS_ROI_CENTRAL_AFRICA["lat_min"]
    lat_max = GLOFAS_ROI_CENTRAL_AFRICA["lat_max"]
    lon_min = GLOFAS_ROI_CENTRAL_AFRICA["lon_min"]
    lon_max = GLOFAS_ROI_CENTRAL_AFRICA["lon_max"]

    restricted_uparea_glofas_v4_0 = restrict_dataset_area(
        uparea_glofas_v4_0,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        buffer,
    )

    out_path = get_path_in_asset(context, settings.tmp_storage, ".nc")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    restricted_uparea_glofas_v4_0.to_netcdf(out_path)
    target_path = get_path_in_asset(context, settings.base_data_upath, ".nc")
    target_path.mkdir(parents=True, exist_ok=True)
    copy_local_file_to_s3(out_path, target_path)

    return None
