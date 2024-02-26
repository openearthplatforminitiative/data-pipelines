import logging

import cdsapi
from dagster import ConfigurableResource
from pydantic import PrivateAttr

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

    def fetch_data(self, request_params, output_path):
        try:
            logging.info("Fetching data from CDS...")
            self._client.retrieve("cems-glofas-forecast", request_params, output_path)
            logging.info(f"CDS data saved to {output_path}")
        except Exception as e:
            logging.error(f"Error fetching data from CDS: {e}")
