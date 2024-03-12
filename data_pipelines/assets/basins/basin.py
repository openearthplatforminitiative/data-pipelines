import io
import zipfile

import httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    MaterializeResult,
    multi_asset,
)

from data_pipelines.settings import settings

HYDROSHEDS_URL = (
    "https://data.hydrosheds.org/file/hydrobasins/standard/hybas_af_lev01-12_v1c.zip"
)
HYDROSHEDS_BASIN_LEVELS = list(range(1, 13))


@multi_asset(
    outs={
        f"basins_level{level:02}": AssetOut(key_prefix="basin")
        for level in HYDROSHEDS_BASIN_LEVELS
    }
)
def basins(context: AssetExecutionContext):
    r = httpx.get(HYDROSHEDS_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    for level in HYDROSHEDS_BASIN_LEVELS:
        file_name = f"hybas_af_lev{level:02}_v1c"
        asset_name = f"basins_level{level:02}"
        for extension in ["dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx"]:
            out_path = settings.base_data_upath.joinpath(
                "basin", "basins", asset_name
            ).with_suffix(extension)
            print(out_path)
            out_path.write_bytes(z.read(file_name))
        yield MaterializeResult(AssetKey(["basin", asset_name]))
