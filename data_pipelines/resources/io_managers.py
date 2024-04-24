from typing import Sequence

import dask.dataframe as dd
import fsspec
import pandas as pd
import rioxarray
import xarray as xr
from dagster import AssetExecutionContext, InputContext, OutputContext, UPathIOManager
from fsspec.implementations.local import LocalFileSystem
from upath import UPath

from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import USE_CONTROL_MEMBER_IN_ENSEMBLE


def get_path_in_asset(
    context: AssetExecutionContext, base_path: UPath, extension: str
) -> UPath:
    path = base_path.joinpath(*context.asset_key.path)
    if context.has_partition_key:
        if len(context.partition_keys) == 1:
            path = path / context.partition_key
        else:
            raise NotImplementedError(
                "get_path_in_asset does not support multi-partition runs."
            )
    return path.with_suffix(extension)


class COGIOManager(UPathIOManager):
    extension: str = ".tif"

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

    def get_path_in_asset(self, context: AssetExecutionContext) -> UPath:
        return get_path_in_asset(context, self._base_path, self.extension)

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray | xr.Dataset | None, path: UPath
    ) -> None:
        obj.to_raster(path)

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        context.log.debug("Loading data using COGIOManager.")
        return rioxarray.open_rasterio(path)


class ZarrIOManager(UPathIOManager):
    extension: str = ".zarr"

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        obj.to_zarr(
            path.as_uri(),
            mode="w",
            storage_options=dict(path.storage_options),
        )

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        return xr.open_dataarray(path)


class DaskParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

    def dump_to_path(
        self, context: OutputContext, obj: pd.DataFrame | dd.DataFrame, path: UPath
    ):
        if isinstance(obj, pd.DataFrame):
            obj.to_parquet(
                path.as_uri(),
                storage_options=path.storage_options,
            )
        elif isinstance(obj, dd.DataFrame):
            obj.to_parquet(
                path.as_uri(),
                storage_options=path.storage_options,
            )
        else:
            raise TypeError("Must be either a Pandas or Dask DataFrame.")

    def load_from_path(
        self, context: InputContext, path: UPath | Sequence[UPath]
    ) -> dd.DataFrame:
        return dd.read_parquet(path.as_uri())

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

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

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
                f"simplecache::{path}",
                filecache={"cache_storage": settings.fsspec_cache_storage},
                **path.storage_options,
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

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath) -> None:
        raise NotImplementedError("NetdCDFIOManager does not support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        return xr.open_dataset(path.open("rb"))
