import os
from typing import Iterable

import dask.dataframe as dd
import rioxarray
import xarray as xr
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
    UPathIOManager,
)
from upath import UPath

from .rio_session import RIOSession


class COGIOManager(ConfigurableIOManager):
    base_path: str
    rio_env: ResourceDependency[RIOSession]

    def get_path(self, context: InputContext | OutputContext) -> str:
        return os.path.join(
            self.base_path,
            *context.asset_key.path,
            f"{context.partition_key}.tif",
        )

    def handle_output(
        self, context: OutputContext, data: xr.DataArray | xr.Dataset | None
    ) -> None:
        context.log.debug("COGIOManager was used.")
        if data is None:
            context.log.info(
                "Received value of None for the data argument in COGIOManager.handle_output. "
                "Skipping output handling, assuming that the output was handled within the asset definition."
            )
            return

        path = self.get_path(context)
        data.to_raster(path)

    def load_input(self, context: InputContext) -> xr.DataArray:
        path = self.get_path(context)
        return rioxarray.open_rasterio(path)


class ZarrIOManager(UPathIOManager):
    extension: str = ".zarr"

    def __init__(self, base_path: str):
        super().__init__(base_path=UPath(base_path))

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        obj.to_zarr(path, mode="w")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        return xr.open_dataarray(path)


class ParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def __init__(self, base_path: str):
        super().__init__(base_path=UPath(base_path))

    def dump_to_path(
        self,
        context: OutputContext,
        obj: dd.DataFrame | Iterable[dd.DataFrame],
        path: UPath,
    ):
        if isinstance(obj, dd.DataFrame):
            obj = [obj]
        df_iter = iter(obj)
        first_df = next(df_iter)
        first_df.to_parquet(path, write_index=False, overwrite=True)

        for df in df_iter:
            df.to_parquet(path, write_index=False, overwrite=False)

    def load_from_path(self, context: InputContext, path: UPath) -> dd.DataFrame:
        return dd.read_parquet(path)
