from dagster import asset
import os
from data_pipelines.utils.flood.config import *


@asset(key_prefix=["flood"], compute_kind="xarray", io_manager_key="netcdf_io_manager")
def upstream_area(context) -> str:
    upstream_file_path = os.path.join(
        PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_UPSTREAM_FILENAME
    )
    return upstream_file_path
