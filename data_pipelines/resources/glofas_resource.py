import logging

import cdsapi
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class CDSClient(ConfigurableResource):
    api_url: str
    api_key: str
    _client: cdsapi.Client = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._client = cdsapi.Client(url=self.api_url, key=self.api_key)

    def fetch_data(self, request_params, output_path):
        try:
            logging.info("Fetching data from CDS...")
            self._client.retrieve("cems-glofas-forecast", request_params, output_path)
            logging.info(f"CDS data saved to {output_path}")
        except Exception as e:
            logging.error(f"Error fetching data from CDS: {e}")
