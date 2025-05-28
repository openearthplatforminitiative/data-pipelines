from dagster import EnvVar

from data_pipelines.resources.dask_resource import (
    DaskFargateResource,
    DaskLocalResource,
    DaskEC2Resource,
)
from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.resources.copernicus_resource import CopernicusClient
from data_pipelines.resources.io_managers import (
    COGIOManager,
    DaskParquetIOManager,
    GribDischargeIOManager,
    NetdCDFIOManager,
    ZarrIOManager,
    JSONIOManager,
)
from data_pipelines.settings import settings

RESOURCES = {
    "dask_resource": (
        DaskLocalResource()
        if settings.run_local
        else DaskFargateResource(
            region_name=settings.aws_region,
            scheduler_task_definition_arn=settings.dask_scheduler_task_definition_arn,
            worker_task_definition_arn=settings.dask_worker_task_definition_arn,
            cluster_arn=settings.dask_ecs_cluster_arn,
            execution_role_arn=settings.dask_execution_role_arn,
            security_groups=settings.dask_security_groups,
            task_role_arn=settings.dask_task_role_arn,
        )
    ),
    "dask_resource_gpu": (
        DaskLocalResource()
        if settings.run_local
        else DaskEC2Resource(
            region=settings.aws_region,
            filesystem_size=settings.dask_ec2_filesystem_size,
            ami=settings.dask_ec2_ami,
            security_groups=settings.dask_security_groups,
            instance_type=settings.dask_ec2_instance_type,
            docker_image=settings.dask_ec2_docker_image,
            key_name=settings.dask_ec2_key_name,
            bootstrap=settings.dask_ec2_bootstrap,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            base_data_path=settings.base_data_path,
        )
    ),
    "cog_io_manager": COGIOManager(base_path=settings.base_data_upath),
    "zarr_io_manager": ZarrIOManager(base_path=settings.base_data_upath),
    "parquet_io_manager": DaskParquetIOManager(base_path=settings.base_data_upath),
    "cds_client": CDSClient(api_key=EnvVar("CDS_API_KEY")),
    "copernicus_client": CopernicusClient(
        user=EnvVar("COPERNICUS_USER"),
        password=EnvVar("COPERNICUS_PASS"),
        datadir=EnvVar("SENTINEL_DATA_DIR"),
    ),
    "grib_io_manager": GribDischargeIOManager(base_path=settings.tmp_storage),
    "netcdf_io_manager": NetdCDFIOManager(base_path=settings.base_data_upath),
    "json_io_manager": JSONIOManager(base_path=settings.base_data_upath),
}
