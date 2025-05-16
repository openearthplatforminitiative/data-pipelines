from pydantic_settings import BaseSettings
from upath import UPath


class Settings(BaseSettings):
    base_data_path: str
    aws_region: str
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    run_local: bool = False
    custom_local_dask_cluster: bool = False
    custom_local_dask_cluster_address: str = "tcp://127.0.0.1:8787"

    tmp_storage: UPath = UPath("/tmp/files")

    dask_scheduler_task_definition_arn: str | None = None
    dask_worker_task_definition_arn: str | None = None
    dask_ecs_cluster_arn: str | None = None
    dask_execution_role_arn: str | None = None
    dask_task_role_arn: str | None = None
    dask_security_group_id: str | None = None

    dask_ec2_ami: str | None = None
    dask_ec2_instance_type: str | None = None
    dask_ec2_docker_image: str | None = None
    dask_ec2_worker_module: str | None = None
    dask_ec2_boostrap: bool | None = None

    @property
    def dask_security_groups(self) -> list[str] | None:
        if self.dask_security_group_id is not None:
            return [self.dask_security_group_id]
        else:
            return None

    @property
    def base_data_upath(self) -> UPath:
        client_kwargs = {"region_name": self.aws_region}
        if self.run_local:
            client_kwargs["endpoint_url"] = "http://host.docker.internal:9000"
        return UPath(
            self.base_data_path,
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
            client_kwargs=client_kwargs,
        )


settings = Settings()
