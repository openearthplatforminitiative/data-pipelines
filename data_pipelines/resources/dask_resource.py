from abc import abstractmethod
from contextlib import contextmanager

from dagster import ConfigurableResource, InitResourceContext, ResourceDependency
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
            self._cluster = cluster
            with Client(cluster) as client:
                self._client = client
                context.log.info("Dask dashboard link: %s", client.dashboard_link)
                yield self
            context.log.debug("Shutting down Dask cluster.")


class DaskLocalResource(DaskResource):
    @contextmanager
    def _provision_cluster(self, context: InitResourceContext, *args, **kwargs):
        context.log.debug("Launching local Dask cluster.")
        with LocalCluster(*args, **kwargs) as cluster:
            yield cluster


class DaskFargateResource(DaskResource):
    n_workers: int = 4
    region: str = settings.aws_region

    @contextmanager
    def _provision_cluster(self, context: InitResourceContext):
        context.log.debug(
            "Launching Dask cluster with %s workers with AWS Fargate.", self.n_workers
        )
        with FargateCluster(
            image=settings.dask_cluster_image,
            n_workers=self.n_workers,
            region_name=self.region,
            task_role_policies=["arn:aws:iam::aws:policy/AmazonS3FullAccess"],
        ) as cluster:
            yield cluster
