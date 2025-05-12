from dagster import asset, AssetExecutionContext
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.assets.sentinel.preprocessing import preprocess_optimize
from sentinel2sr import run
import logging
import os


datapath = os.getenv("SENTINEL_DATA_DIR")
in_dir = f"{datapath}/processed"
out_dir = f"{datapath}/upscaled"
logger = logging.getLogger("upscaling")


@asset(
    deps={"preprocess_optimize": preprocess_optimize},
    key_prefix=["sentinel"],
    compute_kind="dask",
    io_manager_key="json_io_manager",
)
def upscale(context: AssetExecutionContext, dask_resource: DaskResource) -> list:
    tasks = []
    completed_tasks = []

    # Find completed tasks
    os.makedirs(out_dir, exist_ok=True)
    directory = os.fsencode(out_dir)
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if "incomplete" not in filename:
            completed_tasks.append(filename)

    # Find incomplete tasks
    directory = os.fsencode(in_dir)
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename not in completed_tasks:
            tasks.append(f"{in_dir}/{filename}")

    result = dask_resource.submit_subtasks(tasks, _upscaleTile)

    for file in completed_tasks:
        result.append(f"{out_dir}/{file}")

    return result


def _upscaleTile(tile):
    return run("s2v2x2_spatrad", tile, output_dir=out_dir)
