import logging

import cdsapi
import fsspec
from dagster import ConfigurableResource
from pydantic import PrivateAttr
from upath import UPath

from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import GLOFAS_API_URL


class CDSClient(ConfigurableResource):
    api_url: str = GLOFAS_API_URL
    user_id: str
    api_key: str
    _client: cdsapi.Client = PrivateAttr()

    @property
    def _user_key(self) -> str:
        return f"{self.user_id}:{self.api_key}"

    def setup_for_execution(self, context) -> None:
        self._client = cdsapi.Client(url=self.api_url, key=self._user_key)

    def fetch_data(self, request_params, output_path: UPath):
        cached_output_path = fsspec.open_local(
            f"simplecache::{output_path}",
            filecache={"cache_storage": settings.fsspec_cache_storage},
            **output_path.storage_options,
        )
        self._client.retrieve(
            "cems-glofas-forecast", request_params, cached_output_path
        )
