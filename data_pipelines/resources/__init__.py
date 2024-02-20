from dagster import EnvVar

from .dask_resource import DaskResource, DaskLocalResource, DaskFargateResource
from .io_managers import (
    COGIOManager,
    ZarrIOManager,
    ParquetIOManager,
)
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
}
