from dagster import AssetExecutionContext, asset
import os
import httpx
from data_pipelines.utils.flood.config import UPSTREAM_URL


@asset(key_prefix=["flood"], io_manager_key="netcdf_io_manager")
def uparea_glofas_v4_0(context: AssetExecutionContext) -> None:
    # Directory to save the file
    download_dir = os.path.join(
        context.resources.netcdf_io_manager.base_path,
        *context.asset_key.path[:-1],
        context.asset_key.path[-1] + context.resources.netcdf_io_manager.extension,
    )

    response = httpx.get(UPSTREAM_URL)
    # Write the contents of the response to the file
    with open(download_dir, "wb") as file:
        file.write(response.content)

    context.log.info(
        f"File at '{UPSTREAM_URL}' downloaded successfully to '{download_dir}'."
    )
    return None
