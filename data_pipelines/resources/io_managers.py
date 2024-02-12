from typing import Iterable
from upath import UPath
from urllib.request import urlretrieve

import dask.dataframe as dd
from rasterio.io import DatasetReader
import rioxarray
from rio_cogeo import cog_info, cog_profiles, cog_translate
import xarray as xr

from dagster import (
    InputContext,
    OutputContext,
    MetadataValue,
    UPathIOManager,
)

# DATA_BASE_PATH = "s3://openepi-data"
DATA_BASE_PATH = "/home/aleks/projects/OpenEPI/data-pipelines/data"


class COGIOManager(UPathIOManager):
    base_path: str = DATA_BASE_PATH
    extension: str = ".tif"

    def __init__(self, **kwargs):
        super().__init__(base_path=UPath(self.base_path), **kwargs)

    def dump_to_path(
        self, context: OutputContext, obj: DatasetReader, path: UPath
    ) -> None:
        cog_translate(obj, path, cog_profiles["deflate"])

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        return rioxarray.open_rasterio(path)


class GeoTIFFIOManager(UPathIOManager):
    extension: str = ".tif"

    def __init__(self, base_path: str = DATA_BASE_PATH, **kwargs):
        super().__init__(base_path=UPath(base_path), **kwargs)

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath):
        urlretrieve(obj, path)
        info = cog_info(path)
        context.add_output_metadata({"GEO": MetadataValue.json(info.GEO.model_dump())})

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        tile = rioxarray.open_rasterio(path)
        return tile


class ZarrIOManager(UPathIOManager):
    base_path: str = DATA_BASE_PATH
    extension: str = ".zarr"

    def __init__(self, **kwargs):
        super().__init__(base_path=UPath(self.base_path), **kwargs)

    def dump_to_path(
        self, context: OutputContext, obj: xr.DataArray, path: UPath
    ) -> None:
        obj.to_zarr(path, mode="w")

    def load_from_path(self, context: InputContext, path: UPath) -> xr.DataArray:
        return xr.open_dataarray(path)


class ParquetIOManager(UPathIOManager):
    base_path: str = DATA_BASE_PATH
    extension: str = ".parquet"

    def __init__(self, **kwargs):
        super().__init__(base_path=UPath(self.base_path), **kwargs)

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
