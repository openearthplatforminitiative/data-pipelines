import os
from typing import Optional
from urllib.request import urlretrieve

import dask.dataframe as dd
import rioxarray
import xarray as xr
from dagster import (ConfigurableIOManager, InputContext, MetadataValue,
                     OutputContext, UPathIOManager)
from rasterio.io import DatasetReader
from rio_cogeo import cog_info, cog_profiles, cog_translate
from upath import UPath

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


class NetCDFIOManager(ConfigurableIOManager):
    base_path: str

    def _get_path(self, context: InputContext | OutputContext) -> str:
        return os.path.join(
            self.base_path,
            *context.asset_key.path,
            f"{context.asset_partition_key}.nc",
        )

    def handle_output(self, context: OutputContext, data: xr.Dataset) -> None:
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data.to_netcdf(path)

    def load_input(self, context: InputContext) -> xr.Dataset:
        return xr.open_dataset(self._get_path(context))


class GRIBIOManager(ConfigurableIOManager):
    base_path: str

    def _get_path(self, context: InputContext | OutputContext) -> str:
        return os.path.join(
            self.base_path,
            *context.asset_key.path,
            f"{context.asset_partition_key}.grib",
        )

    def handle_output(self, context: OutputContext, data: xr.Dataset) -> None:
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data.to_netcdf(path)

    def load_input(self, context: InputContext) -> xr.Dataset:
        return xr.open_dataset(self._get_path(context))


import os

import pandas
from dagster import (ConfigurableIOManager, InputContext, OutputContext,
                     ResourceDependency)
from dagster import _check as check
from dagster._seven.temp_dir import get_system_temp_directory
from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame as PySparkDataFrame


# taken from https://github.com/dagster-io/dagster/blob/4f06be59e313375953962cf1a55a3459058eb0ca/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py
class PartitionedParquetIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    pyspark: ResourceDependency[PySparkResource]

    @property
    def _base_path(self):
        raise NotImplementedError()

    def handle_output(
        self, context: OutputContext, obj: pandas.DataFrame | PySparkDataFrame
    ):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False)
        elif isinstance(obj, PySparkDataFrame):
            row_count = obj.count()
            obj.write.parquet(path=path, mode="overwrite")
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> PySparkDataFrame | str:
        path = self._get_path(context)
        if context.dagster_type.typing_type == PySparkDataFrame:
            # return pyspark dataframe
            return self.pyspark.spark_session.read.parquet(path)

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: InputContext | OutputContext):
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            return os.path.join(self._base_path, key, f"{partition_str}.pq")
        else:
            return os.path.join(self._base_path, f"{key}.pq")


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    base_path: str = get_system_temp_directory()

    @property
    def _base_path(self):
        return self.base_path


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    s3_bucket: str

    @property
    def _base_path(self):
        return "s3://" + self.s3_bucket
