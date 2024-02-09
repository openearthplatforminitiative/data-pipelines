import logging

import cdsapi
from dagster import Field  # , resource_config
from dagster import ConfigurableResource, StringSource, resource
from pydantic import PrivateAttr

# class CDSClient(ConfigurableResource):
#     name: str = "cems-glofas-forecast"
#     _client: cdsapi.Client = PrivateAttr()
#     api_url: str
#     api_key: str

#     # def __init__(self, api_url, api_key, name="cems-glofas-forecast"):
#     #     self.client = cdsapi.Client(url=api_url, key=api_key)
#     #     self.name = name

#     def setup_for_execution(self, context) -> None:
#         self._client = cdsapi.Client(url=self.api_url, key=self.api_key)

#     def fetch_data(self, request_params, output_path):
#         """
#         Fetch data from CDS.
#         """
#         try:
#             logging.info("Fetching data from CDS...")
#             self._client.retrieve(self.name, request_params, output_path)
#             logging.info(f"CDS data saved to {output_path}")
#         except Exception as e:
#             logging.error(f"Error fetching data from CDS: {e}")


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


# class CDSClientResource(ConfigurableResource):
#     def __init__(self, api_url, api_key, name="cems-glofas-forecast"):
#         self.api_url = api_url
#         self.api_key = api_key
#         self.name = name

#     def get_client(self):
#         return cdsapi.Client(url=self.api_url, key=self.api_key)

#     def fetch_data(self, request_params, output_path):
#         """
#         Fetch data from CDS.
#         """
#         client = self.get_client()
#         try:
#             logging.info("Fetching data from CDS...")
#             client.retrieve(self.name, request_params, output_path)
#             logging.info(f"CDS data saved to {output_path}")
#         except Exception as e:
#             logging.error(f"Error fetching data from CDS: {e}")


class CDSConfig:
    BASE_CONFIG = {
        "system_version": "operational",
        "hydrological_model": "lisflood",
        "product_type": "ensemble_perturbed_forecasts",
        "variable": "river_discharge_in_the_last_24_hours",
        "format": "grib",
    }

    def __init__(
        self,
        year,
        month,
        day,
        leadtime_hour,
        area,
        product_type="ensemble_perturbed_forecasts",
    ):
        assert product_type in [
            "ensemble_perturbed_forecasts",
            "control_forecast",
            ["control_forecast", "ensemble_perturbed_forecasts"],
        ], "Invalid product_type. Should be 'ensemble_perturbed_forecasts' or 'control_forecast' or ['control_forecast', 'ensemble_perturbed_forecasts']."

        self.year = year
        self.month = month
        self.day = day
        self.leadtime_hour = leadtime_hour
        self.area = area
        self.product_type = product_type

    def to_dict(self):
        """Returns the config as a dictionary"""
        config = self.BASE_CONFIG.copy()
        config.update(
            {
                "year": self.year,
                "month": self.month,
                "day": self.day,
                "leadtime_hour": self.leadtime_hour,
                "area": self.area,
                "product_type": self.product_type,
            }
        )
        return config