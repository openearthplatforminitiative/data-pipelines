from .dask_resource import DaskResource
from .io_managers import (
    GeoTIFFIOManager,
    COGIOManager,
    ZarrIOManager,
    ParquetIOManager,
)

RESOURCES = {
    "dask_resource": DaskResource(),
    "geotiff_io_manager": GeoTIFFIOManager(),
    "cog_io_manager": COGIOManager(),
    "zarr_io_manager": ZarrIOManager(),
    "parquet_io_manager": ParquetIOManager(),
}
