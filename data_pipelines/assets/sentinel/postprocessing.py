from dagster import asset, AssetExecutionContext, AssetIn
from upath import UPath
from data_pipelines.assets.sentinel.config import PyramidConfig
from data_pipelines.resources.io_managers import copy_s3_to_disk
from data_pipelines.assets.sentinel.logging import redirect_logs_to_dagster
from data_pipelines.settings import settings
from tqdm import tqdm
import os

datapath = os.getenv("SENTINEL_DATA_DIR")


# @asset(
#    ins={
#        "upscale": AssetIn(key_prefix="sentinel"),
#    },
#    key_prefix=["sentinel"]
# )
# def postprocess_cutline(context: AssetExecutionContext, config: CutlineConfig, upscale: list):
#    os.system("gdalwarp -overwrite -multi -of GTiff -cutline %s -crop_to_cutline %s" %
#              (config.crop_shp_file, ' '.join(upscale)))


def _to_upath(path: str) -> UPath:
    client_kwargs = {"region_name": settings.aws_region}
    if settings.run_local:
        client_kwargs["endpoint_url"] = "http://host.docker.internal:9000"
    return UPath(
        path,
        key=settings.aws_access_key_id,
        secret=settings.aws_secret_access_key,
        client_kwargs=client_kwargs,
    )


@asset(
    ins={
        "upscale": AssetIn(key_prefix="sentinel"),
    },
    key_prefix=["sentinel"],
    io_manager_key="json_io_manager",
)
def postprocess_prepare_disk(context: AssetExecutionContext, upscale: list) -> list:
    redirect_logs_to_dagster()
    local_dir = f"{datapath}/upscaled"
    os.makedirs(local_dir, exist_ok=True)
    files = []

    with tqdm(
        desc="Downloading upscaled products", total=len(upscale), unit="tile"
    ) as progress:
        for s3path in upscale:
            filename = s3path.split("/")[-1]
            copy_s3_to_disk(_to_upath(s3path), f"{local_dir}/{filename}")
            files.append(f"{local_dir}/{filename}")
            progress.update()

    return files


@asset(
    ins={
        "postprocess_prepare_disk": AssetIn(key_prefix="sentinel"),
    },
    key_prefix=["sentinel"],
)
def postprocess_pyramid(
    context: AssetExecutionContext,
    config: PyramidConfig,
    postprocess_prepare_disk: list,
):
    redirect_logs_to_dagster()
    vrt_path = f"{datapath}/finished.vrt"
    pyramid_dir = f"{datapath}/{config.pyramid_folder}"

    os.system("gdalbuildvrt %s %s" % (vrt_path, " ".join(postprocess_prepare_disk)))

    os.makedirs(pyramid_dir, exist_ok=True)

    os.system(
        "gdal_retile.py -v -r bilinear -levels 6 -ps 2048 2048 -co 'TILED=YES' -co 'COMPRESS=JPEG' -targetDir %s %s"
        % (pyramid_dir, vrt_path)
    )


@asset(
    deps={"postprocess_pyramid": postprocess_pyramid},
    key_prefix=["sentinel"],
)
def postprocess_cleanup(
    context: AssetExecutionContext,
):
    pass
