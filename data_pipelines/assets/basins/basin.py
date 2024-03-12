import io
import zipfile

import httpx
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from data_pipelines.settings import settings

HYDROSHEDS_URL = (
    "https://data.hydrosheds.org/file/hydrobasins/standard/hybas_af_lev01-12_v1c.zip"
)
HYDROSHEDS_BASIN_LEVEL = 7


@asset(key_prefix="basin")
def basins(context: AssetExecutionContext):
    r = httpx.get(HYDROSHEDS_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    file_name = f"hybas_af_lev{HYDROSHEDS_BASIN_LEVEL:02}_v1c"
    asset_name = "basins"
    for extension in ["dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx"]:
        out_path = settings.base_data_upath.joinpath(
            "basin", "basins", asset_name
        ).with_suffix(extension)
        print(out_path)
        out_path.write_bytes(z.read(file_name))
    return MaterializeResult(AssetKey(["basin", asset_name]))
