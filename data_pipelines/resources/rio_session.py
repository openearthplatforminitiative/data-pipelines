from abc import abstractmethod
from contextlib import contextmanager

import boto3
import rasterio
import rasterio.session as rio_session
from dagster import ConfigurableResource, InitResourceContext
from rasterio.session import AWSSession


class RIOSession(ConfigurableResource):
    @abstractmethod
    def _get_session(self) -> rio_session.Session:
        """Child classes should overide this method to create an authenticated rasterio session for a speciffic cloud provider."""

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        context.log.debug("Creating rasterio session.")
        session = self._get_session()
        with rasterio.Env(session):
            yield self
        context.log.debug("Shutting down rasterio session.")
        print("Shutting down rasterio session.")


class RIOAWSSession(RIOSession):
    aws_access_key_id: str
    aws_secret_access_key: str

    def _get_session(self) -> rio_session.Session:
        return AWSSession(
            boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name="eu-north-1",
            )
        )
