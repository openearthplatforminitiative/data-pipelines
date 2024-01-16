import os
import shutil
import tempfile
import rioxarray
from rasterio.io import DatasetReader
import xarray as xr
from rio_cogeo import cog_translate, cog_profiles

from dagster import ConfigurableIOManager, InputContext, OutputContext


class GFCTileIOManager(ConfigurableIOManager):
    base_path: str
    chunk_size: int

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
            self._get_path(context), chunks=(1, self.chunk_size, self.chunk_size)
        ).squeeze()  # data is single band, use squeeze to drop band dimension
        return tile


class ZarrIOManager(ConfigurableIOManager):
    base_path: str
    chunk_size: int

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

    def load_input(self, context: InputContext) -> xr.DataArray:
        data = xr.open_dataarray(
            self._get_path(context),
            chunks={"year": 1, "x": self.chunk_size, "y": self.chunk_size},
        )
        return data
