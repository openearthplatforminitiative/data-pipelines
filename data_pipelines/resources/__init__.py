from dagster import EnvVar

from data_pipelines.resources.dask_resource import DaskFargateResource
from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.resources.io_managers import (
    COGIOManager,
    DaskParquetIOManager,
    DummyIOManager,
    GribDischargeIOManager,
    NetdCDFIOManager,
    ZarrIOManager,
)
from data_pipelines.settings import settings

RESOURCES = {
    "dask_resource": DaskFargateResource(
        region_name=settings.aws_region,
        scheduler_task_definition_arn=settings.dask_scheduler_task_definition_arn,
        worker_task_definition_arn=settings.dask_worker_task_definition_arn,
        cluster_arn=settings.dask_ecs_cluster_arn,
        execution_role_arn=settings.dask_execution_role_arn,
        security_groups=settings.dask_security_groups,
        task_role_arn=settings.dask_task_role_arn,
    ),
    "cog_io_manager": COGIOManager(base_path=settings.base_data_upath),
    "zarr_io_manager": ZarrIOManager(base_path=settings.base_data_upath),
    "parquet_io_manager": DaskParquetIOManager(base_path=settings.base_data_upath),
    "cds_client": CDSClient(
        user_id=EnvVar("CDS_USER_ID"), api_key=EnvVar("CDS_API_KEY")
    ),
    "grib_io_manager": GribDischargeIOManager(base_path=settings.tmp_storage),
    "netcdf_io_manager": NetdCDFIOManager(base_path=settings.base_data_upath),
    "dummy_io_manager": DummyIOManager(base_path=settings.base_data_upath),
}
