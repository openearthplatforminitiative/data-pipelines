import os

import httpx
from dagster import AssetExecutionContext, asset

from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import UPSTREAM_URL


@asset(key_prefix=["flood"], compute_kind="xarray", io_manager_key="netcdf_io_manager")
def uparea_glofas_v4_0(context: AssetExecutionContext) -> None:
    # Directory to save the file
    download_dir = (
        os.path.join(
            settings.base_data_path,
            *context.asset_key.path,
        )
        + ".nc"
    )

    response = httpx.get(UPSTREAM_URL)
    # Write the contents of the response to the file
    with open(download_dir, "wb") as file:
        file.write(response.content)

    context.log.info(
        f"File at '{UPSTREAM_URL}' downloaded successfully to '{download_dir}'."
    )
    return None
