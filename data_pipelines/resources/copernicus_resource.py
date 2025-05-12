from dagster import ConfigurableResource
from sentinelloader import Sentinel2Loader
import logging


class CopernicusClient(ConfigurableResource):
    user: str
    password: str
    datadir: str

    @property
    def _user(self) -> str:
        return self.user

    @property
    def _pass(self) -> str:
        return self.password

    @property
    def _datadir(self) -> str:
        return self.datadir

    def setup_for_execution(self, context) -> None:
        self._client = Sentinel2Loader(self.datadir, self.user, self.password, loglevel=logging.INFO,
                                       cloudCoverage=(0, 100), ignoreMissing=False, mosaic=True)

    def findProducts(self, area, year, quartile):
        products = self._client.getAreaMosaics(area, year, quartile)
        return self._client.downloadAll(products, checksum=False)  # checksum false needed for mosaics
