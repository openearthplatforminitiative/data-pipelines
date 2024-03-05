import os

import httpx
from dagster import AssetExecutionContext, asset
from upath import UPath

from data_pipelines.resources.io_managers import get_path_in_asset
from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import UPSTREAM_URL


@asset(key_prefix=["flood"], io_manager_key="netcdf_io_manager")
def uparea_glofas_v4_0(context: AssetExecutionContext) -> None:

    out_path = get_path_in_asset(context, settings.base_data_path, ".nc")
    out_path.mkdir(parents=True, exist_ok=True)

    response = httpx.get(UPSTREAM_URL)
    out_path.write_bytes(response.content)
    return None
