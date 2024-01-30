from dagster import ConfigurableResource, InitResourceContext
from dask.distributed import Client, LocalCluster
from contextlib import contextmanager


class DaskResource(ConfigurableResource):
    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        with LocalCluster() as cluster:
            with Client(cluster) as client:
                context.log.info("Dask dashboard link: %s", client.dashboard_link)
                yield self

    def get_client(self) -> Client:
        print(self.client.dashboard_link)
        return self.client
