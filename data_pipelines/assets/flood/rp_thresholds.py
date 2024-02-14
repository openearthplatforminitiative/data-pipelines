from dagster import AssetExecutionContext, asset
import os
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.etl.raster_converter import RasterConverter
from data_pipelines.utils.flood.etl.utils import add_geometry
import dask.dataframe as dd
import xarray as xr


@asset(
    key_prefix=["flood"], compute_kind="xarray", io_manager_key="new_parquet_io_manager"
)
def rp_2y_thresh_pq(context):
    converter = RasterConverter()
    target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
    os.makedirs(target_parquet_folder, exist_ok=True)

    threshold = GLOFAS_RET_PRD_THRESH_VALS[0]
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

    ds = xr.open_dataset(raster_filepath)
    df = converter.dataset_to_dataframe(ds, cols_to_drop=["wgs_1984"], drop_index=False)
    df = df.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        }
    )

    return df

    converter.file_to_parquet(
        raster_filepath,
        parquet_filepath,
        cols_to_drop=["wgs_1984"],
        cols_to_rename={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        },
        drop_index=False,
        save_index=False,
        context=context,
    )
    # Log the contents of the target folder
    context.log.info(f"Contents of {target_parquet_folder}:")
    context.log.info(os.listdir(target_parquet_folder))


@asset(
    key_prefix=["flood"], compute_kind="xarray", io_manager_key="new_parquet_io_manager"
)
def rp_5y_thresh_pq(context):
    converter = RasterConverter()
    target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
    os.makedirs(target_parquet_folder, exist_ok=True)

    threshold = GLOFAS_RET_PRD_THRESH_VALS[1]
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

    ds = xr.open_dataset(raster_filepath)
    df = converter.dataset_to_dataframe(ds, cols_to_drop=["wgs_1984"], drop_index=False)
    df = df.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        }
    )

    return df

    converter.file_to_parquet(
        raster_filepath,
        parquet_filepath,
        cols_to_drop=["wgs_1984"],
        cols_to_rename={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        },
        drop_index=False,
        save_index=False,
    )
    # Log the contents of the target folder
    context.log.info(f"Contents of {target_parquet_folder}:")
    context.log.info(os.listdir(target_parquet_folder))


@asset(
    key_prefix=["flood"], compute_kind="xarray", io_manager_key="new_parquet_io_manager"
)
def rp_20y_thresh_pq(context):
    converter = RasterConverter()
    target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
    os.makedirs(target_parquet_folder, exist_ok=True)

    threshold = GLOFAS_RET_PRD_THRESH_VALS[2]
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

    ds = xr.open_dataset(raster_filepath)
    df = converter.dataset_to_dataframe(ds, cols_to_drop=["wgs_1984"], drop_index=False)
    df = df.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        }
    )

    return df

    converter.file_to_parquet(
        raster_filepath,
        parquet_filepath,
        cols_to_drop=["wgs_1984"],
        cols_to_rename={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        },
        drop_index=False,
        save_index=False,
    )
    # Log the contents of the target folder
    context.log.info(f"Contents of {target_parquet_folder}:")
    context.log.info(os.listdir(target_parquet_folder))


@asset(
    key_prefix=["flood"],
    # deps=[rp_2y_thresh_pq, rp_5y_thresh_pq, rp_20y_thresh_pq],
    compute_kind="dask",
    io_manager_key="new_parquet_io_manager",
)
def rp_combined_thresh_pq(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    rp_2y_thresh_pq,
    rp_5y_thresh_pq,
    rp_20y_thresh_pq,
):
    dataframes = [rp_2y_thresh_pq, rp_5y_thresh_pq, rp_20y_thresh_pq]

    # Read in dataframes, rename threshold column, add return period column, and round lat/lon values
    # for threshold in GLOFAS_RET_PRD_THRESH_VALS:
    #    threshold_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]
    #     threshold_filepath = os.path.join(
    #        DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, threshold_filename
    #     )
    #
    #     df = dd.read_parquet(threshold_filepath, engine="pyarrow")
    #     df["latitude"] = df["latitude"].round(GLOFAS_PRECISION)
    #    df["longitude"] = df["longitude"].round(GLOFAS_PRECISION)
    #    dataframes.append(df)

    for df in dataframes:
        df["latitude"] = df["latitude"].round(GLOFAS_PRECISION)
        df["longitude"] = df["longitude"].round(GLOFAS_PRECISION)

    # Concatenate dataframes
    combined_df = dataframes[0]
    for next_df in dataframes[1:]:
        combined_df = combined_df.merge(
            next_df, on=["latitude", "longitude"], how="inner"
        )

    # Assuming the rest of the operations are similar and compatible with Dask dataframes
    combined_df = add_geometry(combined_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION)
    sorted_df = combined_df.sort_values(["latitude", "longitude"])

    return sorted_df

    # Write the result
    # target_filepath = os.path.join(
    #    DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME


# )
# sorted_df.to_parquet(target_filepath, engine="pyarrow")

# Log the contents of the target folder
# context.log.info(f"Contents of {target_filepath}:")
# context.log.info(os.listdir(target_filepath))
