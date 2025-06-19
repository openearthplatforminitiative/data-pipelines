from dagster import asset, AssetExecutionContext, AssetIn

from data_pipelines.resources.io_managers import (
    copy_local_file_to_s3,
    get_path_in_asset,
)
from data_pipelines.settings import settings
from data_pipelines.assets.sentinel.logging import redirect_logs_to_dagster
from tqdm import tqdm
import rasterio as rio
import numpy as np
import os
import zipfile
import logging
import hashlib
import shutil

datapath = os.getenv("SENTINEL_DATA_DIR")
zipdir = f"{datapath}/extracted"
logger = logging.getLogger("preprocessing")


@asset(
    ins={
        "raw_imagery": AssetIn(key_prefix="sentinel"),
    },
    key_prefix=["sentinel"],
)
def preprocess_extract(context: AssetExecutionContext, raw_imagery: dict):
    redirect_logs_to_dagster()
    paths = []
    for id in raw_imagery:
        product = raw_imagery[id]
        paths.append(f"{datapath}/products/{product['title']}")

    with tqdm(desc="Unzipping products", total=len(paths), unit="tile") as progress:
        for zipp in paths:
            ziptitle = zipp.split("/")[-1]
            if os.path.exists(f"{zipdir}/{ziptitle}") and not zipfile.is_zipfile(zipp):
                progress.update()
                continue
            with zipfile.ZipFile(zipp, "r") as zipref:
                zipref.extractall(zipdir)
            os.remove(zipp)
            progress.update()


@asset(
    ins={
        "raw_imagery": AssetIn(key_prefix="sentinel"),
    },
    deps={"preprocess_extract": preprocess_extract},
    io_manager_key="json_io_manager",
    key_prefix=["sentinel"],
)
def preprocess_reproject(context: AssetExecutionContext, raw_imagery: dict) -> list:
    redirect_logs_to_dagster()
    reproject_dir = f"{datapath}/reprojected"
    os.makedirs(reproject_dir, exist_ok=True)
    virts = []

    with tqdm(
        desc="Reprojecting image tiles", total=len(raw_imagery), unit="tile"
    ) as progress:
        for id in raw_imagery:
            product = raw_imagery[id]
            bandpaths = f"{zipdir}/{product['title']}"
            filename = f"{reproject_dir}/{product['title']}.tif"
            virts.append(filename)
            if not os.path.exists(filename):
                os.system(
                    f"gdalbuildvrt -q -separate %s %s %s %s %s"
                    % (
                        f"{bandpaths}/rgb.vrt",
                        f"{bandpaths}/B02.tif",
                        f"{bandpaths}/B03.tif",
                        f"{bandpaths}/B04.tif",
                        f"{bandpaths}/B08.tif",
                    )
                )
                os.system(
                    "gdalwarp -q -t_srs EPSG:3857 %s %s"
                    % (f"{bandpaths}/rgb.vrt", filename)
                )

            if os.path.exists(f"{bandpaths}/B02.tif"):
                os.remove(f"{bandpaths}/B02.tif")
            if os.path.exists(f"{bandpaths}/B03.tif"):
                os.remove(f"{bandpaths}/B03.tif")
            if os.path.exists(f"{bandpaths}/B04.tif"):
                os.remove(f"{bandpaths}/B04.tif")
            if os.path.exists(f"{bandpaths}/B08.tif"):
                os.remove(f"{bandpaths}/B08.tif")
            if os.path.exists(f"{bandpaths}/observations.tif"):
                os.remove(f"{bandpaths}/observations.tif")
            progress.update()

    with open(f"{datapath}/retile_inputs.txt", "w") as outfile:
        outfile.write("\n".join(virts))
    return virts


@asset(
    ins={
        "preprocess_reproject": AssetIn(key_prefix="sentinel"),
    },
    key_prefix=["sentinel"],
)
def preprocess_retile(context: AssetExecutionContext, preprocess_reproject: list):
    redirect_logs_to_dagster()
    overlap = 86
    source_tiles = " ".join(preprocess_reproject)
    tilesize = 10008 + overlap * 2

    context.log.info("Building image mosaic VRT")
    os.system("gdalbuildvrt %s %s" % (f"{datapath}/mosaic.vrt", source_tiles))

    context.log.info("Retiling images with tilesize %s" % tilesize)
    os.makedirs(f"{datapath}/retiled", exist_ok=True)
    os.system(
        "gdal_retile.py -v -ps %s %s -overlap %s -resume -targetDir %s %s"
        % (
            tilesize,
            tilesize,
            overlap,
            f"{datapath}/retiled",
            f"{datapath}/mosaic.vrt",
        )
    )
    context.log.info("Deleting input images")
    for file in preprocess_reproject:
        if os.path.exists(file):
            os.remove(file)


@asset(
    deps={"preprocess_retile": preprocess_retile},
    key_prefix=["sentinel"],
    io_manager_key="json_io_manager",
)
def preprocess_optimize(context: AssetExecutionContext) -> list:
    redirect_logs_to_dagster()
    directory = os.fsencode(f"{datapath}/retiled")
    dirlist = os.listdir(directory)
    processeddir = f"{datapath}/processed"
    os.makedirs(processeddir, exist_ok=True)

    processpaths = []
    s3files = []
    nodata_value = -32768
    nodata_count = 0

    with tqdm(desc="Tile optimizing", total=len(dirlist), unit="tile") as progress:
        for file in dirlist:
            filename = os.fsdecode(file)
            filename_full = f"{datapath}/retiled/{filename}"

            with rio.open(filename_full) as src:
                data = src.read(1)
                if np.all(data == nodata_value):
                    os.remove(filename_full)
                    nodata_count += 1
                    progress.update()
                    continue

            file_hash = hashlib.md5(filename.encode()).hexdigest()
            file_full_path = f"{processeddir}/{file_hash}.tif"
            os.system(
                "gdal_translate -q -of COG %s %s" % (filename_full, file_full_path)
            )
            processpaths.append(file_full_path)
            s3path = get_path_in_asset(
                context,
                settings.base_data_upath,
                replace_asset_key=f"preprocessed_data/{file_hash}",
                extension=".tif",
            )
            s3files.append(s3path)
            copy_local_file_to_s3(file_full_path, s3path)
            if os.path.exists(file_full_path):
                os.remove(file_full_path)
            progress.update()

    return s3files


@asset(deps={"preprocess_optimize": preprocess_optimize}, key_prefix=["sentinel"])
def preprocess_cleanup(context: AssetExecutionContext):
    if os.path.isfile(f"{datapath}/mosaic.vrt"):
        os.remove(f"{datapath}/mosaic.vrt")
    if os.path.exists(f"{datapath}/retiled"):
        shutil.rmtree(f"{datapath}/retiled")
    if os.path.exists(f"{zipdir}"):
        shutil.rmtree(f"{zipdir}")
    if os.path.exists(f"{datapath}/reprojected"):
        shutil.rmtree(f"{datapath}/reprojected")
    if os.path.exists(f"{datapath}/processed"):
        shutil.rmtree(f"{datapath}/processed")
    if os.path.exists(f"{datapath}/products"):
        shutil.rmtree(f"{datapath}/products")
    if os.path.exists(f"{datapath}/queries"):
        shutil.rmtree(f"{datapath}/queries")
