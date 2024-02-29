import os
from typing import Sequence

import dask.dataframe as dd
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
from upath import UPath

from data_pipelines.utils.flood.config import USE_CONTROL_MEMBER_IN_ENSEMBLE

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


class DaskParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def __init__(self, base_path: str):
        super().__init__(base_path=UPath(base_path))

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
        raise NotImplementedError(f"GribIOManager does not support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        ds_cf = xr.open_dataset(
            path, backend_kwargs={"filter_by_keys": {"dataType": "cf"}}
        )
        ds_pf = xr.open_dataset(
            path, backend_kwargs={"filter_by_keys": {"dataType": "pf"}}
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
        raise NotImplementedError(
            "This IO Manager doesn't support writing NetCDF data."
        )

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        return xr.open_dataset(path)
