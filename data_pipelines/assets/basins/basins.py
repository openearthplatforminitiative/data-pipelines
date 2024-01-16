import httpx
import os
import zipfile
import io
from dagster import asset


@asset
def basins() -> None:
    url = "https://data.hydrosheds.org/file/hydrobasins/standard/hybas_af_lev01-12_v1c.zip"
    output_path = "data/basins/"
    os.makedirs(output_path, exist_ok=True)

    r = httpx.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(output_path)
