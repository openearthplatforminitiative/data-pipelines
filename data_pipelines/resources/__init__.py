from dagster import EnvVar

from data_pipelines.resources.glofas_resource import CDSClient

from ..settings import settings
from .dask_resource import DaskLocalResource
from .io_managers import (
    COGIOManager,
    GribDischargeIOManager,
    NetdCDFIOManager,
    ParquetIOManager,
    ParquetIOManagerNew,
    ZarrIOManager,
)
from .rio_session import RIOAWSSession

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
    "cds_client": CDSClient(
        user_id=EnvVar("CDS_USER_ID"), api_key=EnvVar("CDS_API_KEY")
    ),
    "grib_io_manager": GribDischargeIOManager(base_path=settings.base_data_path),
    "multi_partition_parquet_io_manager": ParquetIOManagerNew(
        base_path=settings.base_data_path, read_all_partitions=True
    ),
    "new_parquet_io_manager": ParquetIOManagerNew(base_path=settings.base_data_path),
    "netcdf_io_manager": NetdCDFIOManager(base_path=settings.base_data_path),
}
