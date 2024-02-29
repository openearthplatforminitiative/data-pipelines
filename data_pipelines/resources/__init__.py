from dagster import EnvVar
from upath import UPath

from data_pipelines.resources.glofas_resource import CDSClient

from ..settings import settings
from .dask_resource import DaskFargateResource, DaskLocalResource
from .io_managers import (
    COGIOManager,
    DaskParquetIOManager,
    GribDischargeIOManager,
    NetdCDFIOManager,
    ZarrIOManager,
)
from .rio_session import RIOAWSSession

base_data_upath = UPath(
    settings.base_data_path,
    key=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
    secret=EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
)

RESOURCES = {
    "dask_resource": DaskFargateResource(),
    "cog_io_manager": COGIOManager(
        base_path=base_data_upath,
        rio_env=RIOAWSSession(
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        ),
    ),
    "zarr_io_manager": ZarrIOManager(base_path=base_data_upath),
    "parquet_io_manager": DaskParquetIOManager(base_path=base_data_upath),
    "cds_client": CDSClient(
        user_id=EnvVar("CDS_USER_ID"), api_key=EnvVar("CDS_API_KEY")
    ),
    "grib_io_manager": GribDischargeIOManager(base_path=base_data_upath),
    "netcdf_io_manager": NetdCDFIOManager(base_path=base_data_upath),
}
