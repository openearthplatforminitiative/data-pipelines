import json
import os

import requests
from dagster import AssetExecutionContext, MaterializeResult, asset
from dagster_pyspark import PySparkResource
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.etl.raster_converter import RasterConverter
from data_pipelines.utils.flood.spark.transforms import add_geometry

DISCHARGE_DATA_PATH = "/Users/gil/KnowitDecision/Projects/1_OpenEPI/flood-data"


# @asset(key_prefix=["discharge"])
# def rp_2y_thresh_nc() -> MaterializeResult:
#     # Read NetCDF file
#     ds = xr.open_dataset(
#         os.path.join(DISCHARGE_DATA_PATH, "RP2ythresholds_GloFASv40.nc"),
#         engine="netcdf",
#     )
#     # Return the dataset
#     return MaterializeResult(asset_key=None, metadata_entries=[], value=ds)


@asset(key_prefix=["discharge"], compute_kind="xarray")
def rp_2y_thresh_pq(context):
    converter = RasterConverter()
    target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
    os.makedirs(target_parquet_folder, exist_ok=True)

    threshold = GLOFAS_RET_PRD_THRESH_VALS[0]
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

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


@asset(key_prefix=["discharge"], compute_kind="xarray")
def rp_5y_thresh_pq(context):
    converter = RasterConverter()
    target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
    os.makedirs(target_parquet_folder, exist_ok=True)

    threshold = GLOFAS_RET_PRD_THRESH_VALS[1]
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

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


@asset(key_prefix=["discharge"], compute_kind="xarray")
def rp_20y_thresh_pq(context):
    converter = RasterConverter()
    target_parquet_folder = os.path.join(PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH)
    os.makedirs(target_parquet_folder, exist_ok=True)

    threshold = GLOFAS_RET_PRD_THRESH_VALS[2]
    raster_filename = GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES[str(threshold)]
    parquet_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]

    raster_filepath = os.path.join(target_parquet_folder, raster_filename)
    parquet_filepath = os.path.join(target_parquet_folder, parquet_filename)

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
    key_prefix=["discharge"],
    deps=[rp_2y_thresh_pq, rp_5y_thresh_pq, rp_20y_thresh_pq],
    compute_kind="spark",
)
def rp_combined_thresh_pq(context: AssetExecutionContext, pyspark: PySparkResource):
    # spark = SparkSession.builder.getOrCreate()
    spark = pyspark.spark_session
    dataframes = []

    # Read in dataframes, rename threshold column, add return period column, and round lat/lon values
    for threshold in GLOFAS_RET_PRD_THRESH_VALS:
        threshold_filename = GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES[str(threshold)]
        threshold_filepath = os.path.join(
            DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, threshold_filename
        )

        df = (
            spark.read.parquet(threshold_filepath)
            .withColumn("latitude", F.round("latitude", GLOFAS_PRECISION))
            .withColumn("longitude", F.round("longitude", GLOFAS_PRECISION))
        )
        dataframes.append(df)

    # Get a list of counts for each dataframe
    counts = [df.count() for df in dataframes]

    # Check if all counts are the same
    assert (
        len(set(counts)) == 1
    ), f"Not all dataframes have the same count. Counts: {counts}"

    # Store the count of one dataframe for later comparison
    original_count = counts[0]

    # Join the dataframes based on lat and lon
    # Assumes the number of dataframes to join is > 1
    combined_df = dataframes[0]
    for next_df in dataframes[1:]:
        combined_df = combined_df.join(
            next_df, on=["latitude", "longitude"], how="inner"
        )

    # Check if the count after joining is still the same
    combined_row_count = combined_df.count()
    assert (
        combined_row_count == original_count
    ), f"The count after joining ({combined_row_count}) is not the same as before ({original_count})."

    combined_df = add_geometry(combined_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION)
    sorted_df = combined_df.sort(["latitude", "longitude"])
    target_filepath = os.path.join(
        DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME
    )
    sorted_df.write.mode("overwrite").parquet(target_filepath)

    # Log the contents of the target folder
    context.log.info(f"Contents of {target_filepath}:")
    context.log.info(os.listdir(target_filepath))
