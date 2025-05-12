from typing import Sequence

import dask.dataframe as dd
import fsspec
import pandas as pd
import rioxarray
import xarray as xr
from dagster import AssetExecutionContext, InputContext, OutputContext, UPathIOManager
from fsspec.implementations.local import LocalFileSystem
from upath import UPath
import json

from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import USE_CONTROL_MEMBER_IN_ENSEMBLE


def get_path_in_asset(
    context: AssetExecutionContext,
    base_path: UPath,
    extension: str,
    additional_path: str = "",
    input_asset_key: str = "",
    replace_asset_key: str = "",
) -> UPath:
    """
    Get the path to the asset in the asset store.
    If an input asset key is provided, the path will be based on the
    input (upstream dependency) asset key. Otherwise, the path will be
    based on the asset key.

    Args:
        context (AssetExecutionContext): The asset execution context.
        base_path (UPath): The base path to the asset store.
        extension (str): The extension of the file.
        additional_path (str, optional): Additional path to append to the base path. Defaults to "".
        input_asset_key (str, optional): The input asset key. Defaults to "".

    Returns:
        UPath: The path to the asset in the asset store.
    """
    if input_asset_key:
        upstream = context.asset_key_for_input(input_asset_key)
        path = base_path.joinpath(*upstream.path)
    else:
        path = base_path.joinpath(*context.asset_key.path)
        if replace_asset_key:
            # if the asset key contains the replace_asset_key
            # replace the last part of the path with the replace_asset_key
            path = path.parent / replace_asset_key
    if additional_path:
        path = path / additional_path
    if context.has_partition_key:
        if len(context.partition_keys) == 1:
            path = path / context.partition_key
        else:
            raise NotImplementedError(
                "get_path_in_asset does not support multi-partition runs."
            )
    return path.with_suffix(extension)


def get_path_in_io_manager(
    context: InputContext, base_path: UPath, extension: str
) -> UPath:
    path = base_path.joinpath(*context.asset_key.path)
    if context.has_partition_key:
        path = path / context.asset_partition_key
    return path.with_suffix(extension)


def copy_local_file_to_s3(local_file_path: str, target_upath: UPath):
    # Use UPath to copy the local file to the S3 bucket
    with open(local_file_path, "rb") as f:
        with target_upath.open("wb") as s3_f:
            s3_f.write(f.read())


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
                overwrite=True,
                storage_options=path.storage_options,
            )
        else:
            raise TypeError("Must be either a Pandas or Dask DataFrame.")

    def load_from_path(
        self, context: InputContext, path: UPath | Sequence[UPath]
    ) -> dd.DataFrame:
        if isinstance(path, UPath):
            return dd.read_parquet(path.as_uri(), storage_options=path.storage_options)
        elif isinstance(path, (list, tuple)):
            # Assume all elements in the list are UPath instances
            uris = [p.as_uri() for p in path]
            return dd.read_parquet(
                uris, storage_options=path[0].storage_options if path else {}
            )
        else:
            raise ValueError(
                "Path must be a UPath instance or a sequence of UPath instances"
            )

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
        self.base_path = base_path

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        raise NotImplementedError("GribIOManager does not support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        ds_cf = xr.open_dataset(
            path,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"dataType": "cf"}},
        )
        ds_pf = xr.open_dataset(
            path,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"dataType": "pf"}},
        )

        if self.use_control_member_in_ensemble:
            ds_discharge = xr.concat([ds_cf, ds_pf], dim="number")
        else:
            ds_discharge = ds_pf

        return ds_discharge


class NetdCDFIOManager(UPathIOManager):
    extension: str = ".nc"''

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath) -> None:
        raise NotImplementedError("NetdCDFIOManager does not support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        return xr.open_dataset(path.open("rb"))


class JSONIOManager(UPathIOManager):
    extension: str = ".json"

    def __init__(self, base_path: UPath):
        super().__init__(base_path=base_path)

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath) -> None:
        json.dump(obj, path.open(mode="w"), indent=4, sort_keys=True, default=str)

    def load_from_path(self, context: InputContext, path: UPath) -> dict:
        return json.load(path.open())
