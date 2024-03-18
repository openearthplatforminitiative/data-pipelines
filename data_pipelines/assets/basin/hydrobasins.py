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
def hydrobasins(context: AssetExecutionContext) -> MaterializeResult:
    r = httpx.get(HYDROSHEDS_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    file_name = f"hybas_af_lev{HYDROSHEDS_BASIN_LEVEL:02}_v1c"
    asset_name = "hydrobasins"
    for extension in [".dbf", ".prj", ".sbn", ".sbx", ".shp", ".shp.xml", ".shx"]:
        out_path = settings.base_data_upath.joinpath(
            "basin", "hydrobasins", asset_name
        ).with_suffix(extension)
        out_path.write_bytes(z.read(file_name + extension))
    return MaterializeResult(asset_key=AssetKey(["basin", asset_name]))
