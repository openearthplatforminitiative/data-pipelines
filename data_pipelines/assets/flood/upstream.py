import os

import httpx
from dagster import AssetExecutionContext, asset
from upath import UPath

from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import UPSTREAM_URL


def make_path(*args) -> UPath:
    path = UPath(*args)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@asset(key_prefix=["flood"], io_manager_key="netcdf_io_manager")
def uparea_glofas_v4_0(context: AssetExecutionContext) -> None:
    # Directory to save the file
    download_path = make_path(
        settings.base_data_path,
        *context.asset_key.path,
    ).with_suffix(".nc")
    response = httpx.get(UPSTREAM_URL)
    # Write the contents of the response to the file
    download_path.write_bytes(response.content)

    context.log.info(
        f"File at '{UPSTREAM_URL}' downloaded successfully to '{download_path}'."
    )
    return None
