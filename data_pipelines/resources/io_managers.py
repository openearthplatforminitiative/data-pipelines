import os
from typing import Sequence

import dask.dataframe as dd
import fsspec
import pandas as pd
import rioxarray
import xarray as xr
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
    UPathIOManager,
)
from fsspec.implementations.local import LocalFileSystem
from upath import UPath

from data_pipelines.utils.flood.config import USE_CONTROL_MEMBER_IN_ENSEMBLE

from .rio_session import RIOSession


class COGIOManager(ConfigurableIOManager):
    rio_env: ResourceDependency[RIOSession]
    base_path: ResourceDependency[UPath]

    def get_path(self, context: InputContext | OutputContext) -> str:
        return self.base_path.joinpath(
            *context.asset_key.path, context.partition_key
        ).with_suffix(".tif")

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
        context.log.debug("Loading data using COGIOManager.")
        path = self.get_path(context)
        # with self.rio_env.session(context):
        return rioxarray.open_rasterio(path)


class ZarrIOManager(UPathIOManager):
    base_path: ResourceDependency[UPath]
    extension: str = ".zarr"

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        obj.to_zarr(path, mode="w")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        return xr.open_dataarray(path)


class DaskParquetIOManager(UPathIOManager):
    base_path: ResourceDependency[UPath]
    extension: str = ".parquet"

    def dump_to_path(
        self, context: OutputContext, obj: pd.DataFrame | dd.DataFrame, path: UPath
    ):
        if isinstance(obj, pd.DataFrame):
            obj.to_parquet(path)
        else:
            obj.to_parquet(path, overwrite=True)

    def load_from_path(
        self, context: InputContext, path: UPath | Sequence[UPath]
    ) -> dd.DataFrame:
        return dd.read_parquet(path)

    def load_input(self, context: InputContext) -> dd.DataFrame:
        if not context.has_asset_partitions:
            path = self._get_path(context)
            return self._load_single_input(path, context)
        else:
            paths = self._get_paths_for_partitions(context)
            return self.load_from_path(context, list(paths.values()))


class GribDischargeIOManager(UPathIOManager):
    use_control_member_in_ensemble: int = USE_CONTROL_MEMBER_IN_ENSEMBLE
    extension: str = ".grib"

    def __init__(self, base_path: str):
        super().__init__(base_path=UPath(base_path))

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        raise NotImplementedError("GribIOManager does not support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        if isinstance(self.fs, LocalFileSystem):
            ds_source = path
        else:
            # grib files can not be read directly from cloud storage.
            # The file is instead cached and read locally
            # ref: https://stackoverflow.com/questions/66229140/xarray-read-remote-grib-file-on-s3-using-cfgrib
            ds_source = fsspec.open_local(
                f"simplecache::{path}", filecache={"cache_storage": "/tmp/files"}
            )

        ds_cf = xr.open_dataset(
            ds_source,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"dataType": "cf"}},
        )
        ds_pf = xr.open_dataset(
            ds_source,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"dataType": "pf"}},
        )

        if self.use_control_member_in_ensemble:
            ds_discharge = xr.concat([ds_cf, ds_pf], dim="number")
        else:
            ds_discharge = ds_pf

        return ds_discharge


class NetdCDFIOManager(UPathIOManager):
    extension: str = ".nc"

    def __init__(self, base_path: str):
        super().__init__(base_path=UPath(base_path))

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath) -> None:
        raise NotImplementedError("NetdCDFIOManager does not support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        return xr.open_dataset(path.open("rb"))
