from dagster import asset, AssetExecutionContext, AssetIn

from data_pipelines.assets.sentinel.config import UpscaleConfig
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.io_managers import copy_s3_to_disk, copy_local_file_to_s3
from data_pipelines.assets.sentinel.logging import redirect_logs_to_dagster
from data_pipelines.settings import settings
from sentinel2sr import run
import os


@asset(
    ins={"preprocess_optimize": AssetIn(key_prefix="sentinel")},
    key_prefix=["sentinel"],
    compute_kind="dask",
    io_manager_key="json_io_manager",
)
def upscale(
    context: AssetExecutionContext,
    config: UpscaleConfig,
    dask_resource_gpu: DaskResource,
    preprocess_optimize: list,
) -> list:
    redirect_logs_to_dagster()
    tasks = preprocess_optimize
    completed_tasks = []

    # Find completed tasks
    # os.makedirs(out_dir, exist_ok=True)
    # directory = os.fsencode(out_dir)
    # for file in os.listdir(directory):
    #    filename = os.fsdecode(file)
    #    if "incomplete" not in filename:
    #        completed_tasks.append(filename)

    ## Find incomplete tasks
    # directory = os.fsencode(in_dir)
    # for file in os.listdir(directory):
    #    filename = os.fsdecode(file)
    #    if filename not in completed_tasks:
    #        tasks.append(f"{in_dir}/{filename}")

    result = dask_resource_gpu.submit_subtasks(tasks, _upscaleTile, model=config.model)

    # for file in completed_tasks:
    #    result.append(f"{out_dir}/{file}")

    return result


def _upscaleTile(tile, model):
    local_file_basename = tile.split("/")[-1]

    in_file = f"./input/{local_file_basename}"
    os.makedirs(os.path.abspath(os.path.join(in_file, os.pardir)), exist_ok=True)

    out_dir = f"./upscaled"
    os.makedirs(out_dir, exist_ok=True)

    s3tile = (
        settings.base_data_upath / "sentinel/preprocessed_data" / local_file_basename
    )
    copy_s3_to_disk(s3tile, in_file)

    print(f"Upscaling tile: {in_file}")
    upscaled = run(model, in_file, output_dir=out_dir)
    s3path = settings.base_data_upath / f"sentinel/upscaled/{local_file_basename}"
    copy_local_file_to_s3(upscaled, s3path)

    if os.path.isfile(os.path.abspath(in_file)):
        os.remove(os.path.abspath(in_file))
    if os.path.isfile(os.path.abspath(upscaled)):
        os.remove(os.path.abspath(upscaled))
    return s3path
