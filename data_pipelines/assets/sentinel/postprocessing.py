from dagster import asset, AssetExecutionContext, AssetIn
from upath import UPath
from data_pipelines.assets.sentinel.config import MoveFilesConfig
from data_pipelines.resources.io_managers import copy_s3_to_disk
from data_pipelines.assets.sentinel.logging import redirect_logs_to_dagster
from data_pipelines.settings import settings
from tqdm import tqdm
import os
import shutil

datapath = os.getenv("SENTINEL_DATA_DIR")
servepath = os.getenv("SENTINEL_SERVING_DIR")


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
            if not os.path.exists(f"{local_dir}/{filename}"):
                copy_s3_to_disk(_to_upath(s3path), f"{local_dir}/{filename}")
            files.append(f"{local_dir}/{filename}")
            progress.update()

    return files


@asset(
    ins={"postprocess_prepare_disk": AssetIn(key_prefix="sentinel")},
    key_prefix=["sentinel"],
)
def postprocess_pyramid(
    context: AssetExecutionContext,
    postprocess_prepare_disk: list,
):
    redirect_logs_to_dagster()
    pyramid_dir = f"{datapath}/pyramid"

    with open(f"{datapath}/pyramid_inputs.txt", "w") as outfile:
        outfile.write("\n".join(postprocess_prepare_disk))

    os.makedirs(pyramid_dir, exist_ok=True)

    os.system(
        "gdal_retile.py -v -r cubic -levels 11 -ps 2048 2048 -co 'TILED=YES' -co 'COMPRESS=JPEG' -resume --optfile %s -targetDir %s"
        % (f"{datapath}/pyramid_inputs.txt", pyramid_dir)
    )


@asset(
    deps={"postprocess_pyramid": postprocess_pyramid},
    key_prefix=["sentinel"],
)
def postprocess_move_files_and_cleanup(
    context: AssetExecutionContext,
    config: MoveFilesConfig,
):
    if not os.path.exists(f"{datapath}/pyramid"):
        raise NotADirectoryError("pyramid directory does not exist")

    shutil.move(f"{datapath}/pyramid", f"{servepath}/{config.serve_disk_dir}")

    context.log.info("Cleaning up loose files")
    if os.path.exists(f"{datapath}/upscaled"):
        shutil.rmtree(f"{datapath}/upscaled")
    if os.path.exists(f"{datapath}/pyramid_inputs.txt"):
        os.remove(f"{datapath}/pyramid_inputs.txt")
    if os.path.exists(f"{datapath}/retile_inputs.txt"):
        os.remove(f"{datapath}/retile_inputs.txt")
