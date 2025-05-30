from abc import abstractmethod
from contextlib import contextmanager

from dagster import ConfigurableResource, InitResourceContext
from dask.distributed import Client, LocalCluster
from dask_cloudprovider.aws import FargateCluster
from pydantic import PrivateAttr

from data_pipelines.settings import settings


class DaskResource(ConfigurableResource):
    _cluster = PrivateAttr()
    _client: Client = PrivateAttr()

    @contextmanager
    @abstractmethod
    def _provision_cluster(self, context: InitResourceContext, *args, **kwargs):
        """Child classes should override this method to provision a Dask cluster."""

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        with self._provision_cluster(context) as cluster:
            if settings.run_local and settings.custom_local_dask_cluster:
                cluster = settings.custom_local_dask_cluster_address
            self._cluster = cluster
            with Client(cluster) as client:
                self._client = client
                client.upload_file("data_pipelines.zip")
                context.log.info("Dask dashboard link: %s", client.dashboard_link)
                yield self
            context.log.debug("Shutting down Dask cluster.")

    def submit_subtasks(self, tasks: list, handler) -> list:
        futures = [self._client.submit(handler, task) for task in tasks]
        return self._client.gather(futures)


class DaskLocalResource(DaskResource):
    @contextmanager
    def _provision_cluster(self, context: InitResourceContext, *args, **kwargs):
        context.log.debug("Launching local Dask cluster.")
        if settings.run_local and settings.custom_local_dask_cluster:
            yield 1
        else:
            with LocalCluster(
                n_workers=1, threads_per_worker=1, memory_limit="4GB", **kwargs
            ) as cluster:
                yield cluster


class DaskFargateResource(DaskResource):
    region_name: str
    n_workers: int = 4
    scheduler_task_definition_arn: str | None = None
    worker_task_definition_arn: str | None = None
    cluster_arn: str | None = None
    execution_role_arn: str | None = None
    task_role_arn: str | None = None
    security_groups: list[str] | None = None
    image: str | None
    task_role_policies: list[str] | None = None

    @property
    def aws_resources_provided(self) -> bool:
        return None not in [
            self.cluster_arn,
            self.scheduler_task_definition_arn,
            self.worker_task_definition_arn,
            self.security_groups,
            self.execution_role_arn,
            self.task_role_arn,
        ]

    @contextmanager
    def _provision_cluster(self, context: InitResourceContext):
        if self.aws_resources_provided:
            context.log.info(
                "Launching Dask cluster with %s workers with AWS Fargate.",
                self.n_workers,
            )
            with FargateCluster(
                n_workers=self.n_workers,
                region_name=self.region_name,
                scheduler_task_definition_arn=self.scheduler_task_definition_arn,
                worker_task_definition_arn=self.worker_task_definition_arn,
                cluster_arn=self.cluster_arn,
                execution_role_arn=self.execution_role_arn,
                task_role_arn=self.task_role_arn,
                security_groups=self.security_groups,
                skip_cleanup=True,
            ) as cluster:
                yield cluster
        else:
            context.log.warning(
                "Dask cluster ARN config could not be found. Launching ephemeral Dask cluster on AWS Fargate."
            )
            with FargateCluster(
                n_workers=self.n_workers,
                region_name=self.region_name,
                image=self.image,
                task_role_policies=self.task_role_policies,
            ) as cluster:
                yield cluster
