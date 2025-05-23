from dagster import asset, AssetExecutionContext, AssetIn
from data_pipelines.resources.dask_resource import DaskResource
from tqdm import tqdm
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
    paths = []
    for id in raw_imagery:
        product = raw_imagery[id]
        paths.append(f"{datapath}/products/{product['title']}")

    with tqdm(desc="Unzipping products", total=len(paths), unit="tile") as progress:
        for zipp in paths:
            ziptitle = zipp.split("/")[-1]
            if os.path.exists(f"{zipdir}/{ziptitle}"):
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
    compute_kind="dask",
    key_prefix=["sentinel"],
)
def preprocess_reproject(
    context: AssetExecutionContext, dask_resource: DaskResource, raw_imagery: dict
) -> list:
    virts = []

    with tqdm(
        desc="Reprojecting image tiles", total=len(raw_imagery), unit="tile"
    ) as progress:
        for id in raw_imagery:
            product = raw_imagery[id]
            bandpaths = f"{zipdir}/{product['title']}"
            virts.append(f"{bandpaths}/tile.tif")
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
                % (f"{bandpaths}/rgb.vrt", f"{bandpaths}/tile.tif")
            )
            progress.update()

    return virts


@asset(
    ins={
        "preprocess_reproject": AssetIn(key_prefix="sentinel"),
    },
    key_prefix=["sentinel"],
)
def preprocess_retile(context: AssetExecutionContext, preprocess_reproject: list):
    overlap = 86
    virts = preprocess_reproject
    source_tiles = " ".join(virts)
    tilesize = 10008 + overlap * 2

    logger.info("Building image mosaic VRT")
    os.system("gdalbuildvrt %s %s" % (f"{datapath}/mosaic.vrt", source_tiles))

    logger.info("Retiling images with tilesize %s" % tilesize)
    os.makedirs(f"{datapath}/retiled", exist_ok=True)
    os.system(
        "gdal_retile -ps %s %s -overlap %s -targetDir %s %s"
        % (tilesize, tilesize, overlap, f"{datapath}/retiled", f"{datapath}/mosaic.vrt")
    )


@asset(deps={"preprocess_retile": preprocess_retile}, key_prefix=["sentinel"])
def preprocess_optimize(context: AssetExecutionContext):
    directory = os.fsencode(f"{datapath}/retiled")
    dirlist = os.listdir(directory)
    processeddir = f"{datapath}/processed"
    os.makedirs(processeddir, exist_ok=True)

    processpaths = []

    with tqdm(desc="Tile optimizing", total=len(dirlist), unit="tile") as progress:
        for file in dirlist:
            filename = os.fsdecode(file)
            filename_full = f"{datapath}/retiled/{filename}"
            file_hash = hashlib.md5(filename.encode()).hexdigest()
            os.system(
                "gdal_translate -q -of COG %s %s"
                % (filename_full, f"{processeddir}/{file_hash}.tif")
            )
            os.remove(filename_full)
            processpaths.append(f"{processeddir}/{file_hash}.tif")
            progress.update()


@asset(deps={"preprocess_optimize": preprocess_optimize}, key_prefix=["sentinel"])
def preprocess_cleanup(context: AssetExecutionContext):
    if os.path.isfile(f"{datapath}/mosaic.vrt"):
        os.remove(f"{datapath}/mosaic.vrt")
    if os.path.exists(f"{datapath}/retiled"):
        shutil.rmtree(f"{datapath}/retiled")
    if os.path.exists(f"{zipdir}"):
        shutil.rmtree(f"{zipdir}")
