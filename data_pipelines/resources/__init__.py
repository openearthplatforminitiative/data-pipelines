from data_pipelines.utils.flood.config import GLOFAS_API_URL
from dagster import EnvVar

from .dask_resource import DaskLocalResource
from .io_managers import (
    COGIOManager,
    GribIOManager,
    ParquetIOManagerNew,
    ZarrIOManager,
    ParquetIOManager,
    NetdCDFIOManager,
)
from data_pipelines.resources.glofas_resource import CDSClient
from .rio_session import RIOAWSSession
from ..settings import settings


RESOURCES = {
    "dask_resource": DaskLocalResource(),
    "cog_io_manager": COGIOManager(
        base_path=settings.base_data_path,
        rio_session=RIOAWSSession(
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=EnvVar("AWS_SESSION_TOKEN"),
        ),
    ),
    "zarr_io_manager": ZarrIOManager(base_path=settings.base_data_path),
    "parquet_io_manager": ParquetIOManager(base_path=settings.base_data_path),
    "client": CDSClient(
        api_url=GLOFAS_API_URL,
        api_key=f"{EnvVar('CDS_USER_ID').get_value()}:{EnvVar('CDS_API_KEY').get_value()}",
    ),
    "grib_io_manager": GribIOManager(),
    "multi_partition_parquet_io_manager": ParquetIOManagerNew(read_all_partitions=True),
    "new_parquet_io_manager": ParquetIOManagerNew(),
    "netcdf_io_manager": NetdCDFIOManager(),
}
