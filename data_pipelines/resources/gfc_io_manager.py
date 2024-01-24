import os
import rioxarray
from rasterio.io import DatasetReader
import xarray as xr
from rio_cogeo import cog_info, cog_profiles, cog_translate
from urllib.request import urlretrieve

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    MetadataValue,
)


class COGIOManager(ConfigurableIOManager):
    base_path: str

    def _get_path(self, context: InputContext | OutputContext) -> str:
        return os.path.join(
            self.base_path,
            *context.asset_key.path,
            f"{context.asset_partition_key}.tif",
        )

    def handle_output(self, context: OutputContext, data: DatasetReader) -> None:
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        cog_translate(data, path, cog_profiles["deflate"])
        data.close()

    def load_input(self, context: InputContext) -> xr.DataArray:
        tile = rioxarray.open_rasterio(
            self._get_path(context),
        ).squeeze()  # data is single band, use squeeze to drop band dimension
        return tile


class GeoTIFFIOManager(ConfigurableIOManager):
    base_path: str

    def _get_path(self, context: InputContext | OutputContext) -> str:
        return os.path.join(
            self.base_path,
            *context.asset_key.path,
            f"{context.asset_partition_key}.tif",
        )

    def handle_output(self, context: OutputContext, url: str) -> None:
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        urlretrieve(url, path)
        info = cog_info(path)
        context.add_output_metadata({"GEO": MetadataValue.json(info.GEO.model_dump())})

    def load_input(self, context: InputContext) -> xr.DataArray:
        tile = rioxarray.open_rasterio(
            self._get_path(context),
        ).squeeze()  # data is single band, use squeeze to drop band dimension
        return tile


class ZarrIOManager(ConfigurableIOManager):
    base_path: str

    def _get_path(self, context: InputContext | OutputContext) -> str:
        return os.path.join(
            self.base_path,
            *context.asset_key.path,
            f"{context.asset_partition_key}.zarr",
        )

    def handle_output(self, context: OutputContext, data: xr.DataArray) -> None:
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data.to_zarr(path, mode="w")
        data.close()

    def load_input(self, context: InputContext) -> xr.DataArray:
        data = xr.open_dataarray(
            self._get_path(context),
        )
        return data
