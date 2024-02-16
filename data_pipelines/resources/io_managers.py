import os
from typing import Any, Dict, Iterable
import pandas as pd
from upath import UPath
from urllib.request import urlretrieve

import dask.dataframe as dd
from rasterio.io import DatasetReader
import rioxarray
from rio_cogeo import cog_info, cog_profiles, cog_translate
import xarray as xr

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    MetadataValue,
    UPathIOManager,
)

from data_pipelines.utils.flood.config import (
    OPENEPI_BASE_PATH,
    USE_CONTROL_MEMBER_IN_ENSEMBLE,
)

DATA_BASE_PATH = "/home/aleks/projects/OpenEPI/data-pipelines/data"


class COGIOManager(ConfigurableIOManager):
    base_path: str = DATA_BASE_PATH

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
    base_path: str = DATA_BASE_PATH

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
    base_path: str = DATA_BASE_PATH

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
        )
        return data


class ParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def __init__(self, base_path: UPath | None = None):
        base_path = base_path or UPath(DATA_BASE_PATH)
        super().__init__(base_path)

    def dump_to_path(self, context: OutputContext, obj: dd.DataFrame, path: UPath):
        obj.to_parquet(path, write_index=False, overwrite=True)

    def load_from_path(self, context: InputContext, path: UPath) -> dd.DataFrame:
        return dd.read_parquet(path)


class GribIOManager(UPathIOManager):
    base_path: str = OPENEPI_BASE_PATH
    use_control_member_in_ensemble: int = USE_CONTROL_MEMBER_IN_ENSEMBLE
    extension: str = ".grib"

    def __init__(self, **kwargs):
        super().__init__(base_path=UPath(self.base_path), **kwargs)

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        raise NotImplementedError("This IO Manager doesn't support writing GRIB data.")

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


class ParquetIOManagerNew(UPathIOManager):
    base_path: str = OPENEPI_BASE_PATH
    extension: str = ".parquet"
    engine: str = "pyarrow"
    compression: str = "snappy"
    read_all_partitions: bool = False

    def __init__(self, **kwargs):
        if "read_all_partitions" in kwargs:
            self.read_all_partitions = kwargs.pop("read_all_partitions")
        super().__init__(base_path=UPath(self.base_path), **kwargs)

    def dump_to_path(
        self,
        context: OutputContext,
        obj: (
            dd.DataFrame
            | Iterable[dd.DataFrame]
            | pd.DataFrame
            | Iterable[pd.DataFrame]
        ),
        path: UPath,
    ):
        if isinstance(obj, (dd.DataFrame, pd.DataFrame)):
            obj = [obj]
        df_iter = iter(obj)
        first_df = next(df_iter)

        kwargs = {"engine": self.engine, "compression": self.compression}

        # Could be a pd.DataFrame or a dd.DataFrame
        # Need to use "index" for pd.DataFrame and "write_index" for dd.DataFrame
        if isinstance(first_df, pd.DataFrame):
            kwargs["index"] = False
        else:
            kwargs["write_index"] = False
            kwargs["overwrite"] = True
        first_df.to_parquet(path, **kwargs)

        for df in df_iter:
            df.to_parquet(path, **kwargs)

    def load_from_path(self, context: InputContext, path: UPath) -> dd.DataFrame:
        return dd.read_parquet(path, engine=self.engine)

    def load_input(self, context: InputContext) -> Any | Dict[str, Any]:
        if (
            self.read_all_partitions
            and context.has_asset_partitions
            and context.dagster_type.typing_type != dict
        ):
            path = self._get_path_without_extension(context)
            path = UPath(
                os.path.join(
                    path,
                    "*.parquet",
                )
            )
            context.log.info(f"Loading all partitions from {path}")
            return self.load_from_path(context=context, path=path)
        else:
            return super().load_input(context)


class NetdCDFIOManager(UPathIOManager):
    base_path: str = OPENEPI_BASE_PATH
    extension: str = ".nc"

    def __init__(self, **kwargs):
        super().__init__(base_path=UPath(self.base_path), **kwargs)

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath) -> None:
        raise NotImplementedError(
            "This IO Manager doesn't support writing NetCDF data."
        )

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        return xr.open_dataset(path)
