from dagster import EnvVar

from data_pipelines.resources.dask_resource import (
    DaskFargateResource,
    DaskLocalResource,
)
from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.resources.io_managers import (
    COGIOManager,
    DaskParquetIOManager,
    GribDischargeIOManager,
    NetdCDFIOManager,
    ZarrIOManager,
)
from data_pipelines.settings import settings

BASE_DATA_PATH = settings.base_data_upath

RESOURCES = {
    "dask_resource": DaskFargateResource(
        region_name=settings.aws_region,
        scheduler_task_definition_arn=settings.dask_scheduler_task_definition_arn,
        worker_task_definition_arn=settings.dask_worker_task_definition_arn,
        cluster_arn=settings.dask_cluster_arn,
        execution_role_arn=settings.dask_execution_role_arn,
        security_groups=settings.dask_security_groups,
        task_role_arn=settings.dask_task_role_arn,
    ),
    "cog_io_manager": COGIOManager(base_path=BASE_DATA_PATH),
    "zarr_io_manager": ZarrIOManager(base_path=BASE_DATA_PATH),
    "parquet_io_manager": DaskParquetIOManager(base_path=BASE_DATA_PATH),
    "cds_client": CDSClient(
        user_id=EnvVar("CDS_USER_ID"), api_key=EnvVar("CDS_API_KEY")
    ),
    "grib_io_manager": GribDischargeIOManager(base_path=BASE_DATA_PATH),
    "netcdf_io_manager": NetdCDFIOManager(base_path=BASE_DATA_PATH),
}
