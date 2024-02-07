import os
from datetime import datetime, timedelta

import xarray as xr
from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset
from dagster_pyspark import PySparkResource
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, LongType, StructField, StructType
from tqdm import tqdm

from data_pipelines.assets.discharge.rp_threshold import rp_combined_thresh_pq
from data_pipelines.resources.glofas_resource import CDSClient, CDSConfig
from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.etl.filter_by_upstream import apply_upstream_threshold
from data_pipelines.utils.flood.etl.raster_converter import RasterConverter
from data_pipelines.utils.flood.etl.utils import restrict_dataset_area
from data_pipelines.utils.flood.spark.transforms import (
    add_geometry,
    compute_flood_intensity,
    compute_flood_peak_timing,
    compute_flood_tendency,
    compute_flood_threshold_percentages,
)


@asset(key_prefix=["discharge"])
def discharge(context: AssetExecutionContext, client: CDSClient) -> MaterializeResult:
    date_for_request = datetime.utcnow()  # - timedelta(days=1)
    formatted_date = date_for_request.strftime("%Y-%m-%d")

    query_buffer = GLOFAS_RESOLUTION * GLOFAS_BUFFER_MULT
    lat_min = GLOFAS_ROI_CENTRAL_AFRICA["lat_min"]
    lat_max = GLOFAS_ROI_CENTRAL_AFRICA["lat_max"]
    lon_min = GLOFAS_ROI_CENTRAL_AFRICA["lon_min"]
    lon_max = GLOFAS_ROI_CENTRAL_AFRICA["lon_max"]

    area = [
        lat_max + query_buffer,
        lon_min - query_buffer,
        lat_min - query_buffer,
        lon_max + query_buffer,
    ]

    if USE_CONTROL_MEMBER_IN_ENSEMBLE:
        product_type = ["control_forecast", "ensemble_perturbed_forecasts"]
        print("Retrieving both control and ensemble")
    else:
        product_type = "ensemble_perturbed_forecasts"
        print("Retrieving only ensemble")

    # Create client
    # client = CDSClient(GLOFAS_API_URL, f"{user_id}:{api_key}")

    target_folder = os.path.join(
        PYTHON_PREFIX, S3_GLOFAS_DOWNLOADS_PATH, formatted_date
    )
    os.makedirs(target_folder, exist_ok=True)

    for l_hour in LEADTIME_HOURS:
        # Define target filepath
        target_filename = f"download-{l_hour}.grib"
        target_file_path = os.path.join(target_folder, target_filename)

        # Define the config
        config = CDSConfig(
            year=date_for_request.year,
            month=date_for_request.month,
            day=date_for_request.day,
            leadtime_hour=l_hour,
            area=area,
            product_type=product_type,
        )

        # Convert config to a dictionary
        request_params = config.to_dict()

        # Fetch the data
        client.fetch_data(request_params, target_file_path)

    # get the list of files in the folder
    files = os.listdir(target_folder)

    # Log the contents of the target folder
    context.log.info(f"Contents of {target_folder}:")
    context.log.info(files)

    return MaterializeResult(
        metadata={
            "date": formatted_date,
            "area": area,
            "number_of_assets": len(files),
            "path": target_folder,
        }
    )


@asset(key_prefix=["discharge"], deps=[discharge], compute_kind="xarray")
def transformed_discharge(context: AssetExecutionContext):
    date_for_request = datetime.utcnow() - timedelta(days=2)
    formatted_date = date_for_request.strftime("%Y-%m-%d")

    buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV
    lat_min = GLOFAS_ROI_CENTRAL_AFRICA["lat_min"]
    lat_max = GLOFAS_ROI_CENTRAL_AFRICA["lat_max"]
    lon_min = GLOFAS_ROI_CENTRAL_AFRICA["lon_min"]
    lon_max = GLOFAS_ROI_CENTRAL_AFRICA["lon_max"]

    converter = RasterConverter()

    # Create target folder
    filtered_parquet_folder = os.path.join(
        PYTHON_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date
    )
    os.makedirs(filtered_parquet_folder, exist_ok=True)

    # Open upstream area NetCDF file and restrict to area of interest
    upstream_file_path = os.path.join(
        PYTHON_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_UPSTREAM_FILENAME
    )
    ds_upstream = xr.open_dataset(upstream_file_path)

    if USE_CONTROL_MEMBER_IN_ENSEMBLE:
        print("Combining control and ensemble")
    else:
        print("Using only ensemble")

    for l_hour in tqdm(LEADTIME_HOURS):
        discharge_filename = f"download-{l_hour}.grib"
        discharge_file_path = os.path.join(
            PYTHON_PREFIX, S3_GLOFAS_DOWNLOADS_PATH, formatted_date, discharge_filename
        )

        # Load control forecast (cf)
        # Could be emty if it wasn't downloaded i API query
        ds_cf = xr.open_dataset(
            discharge_file_path, backend_kwargs={"filter_by_keys": {"dataType": "cf"}}
        )

        # Load perturbed forecast (pf)
        ds_pf = xr.open_dataset(
            discharge_file_path, backend_kwargs={"filter_by_keys": {"dataType": "pf"}}
        )

        if USE_CONTROL_MEMBER_IN_ENSEMBLE:
            ds_discharge = xr.concat([ds_cf, ds_pf], dim="number")
        else:
            ds_discharge = ds_pf

        # ds_discharge = open_dataset(discharge_file_path)

        # Restrict discharge data to area of interest
        ds_discharge = restrict_dataset_area(
            ds_discharge, lat_min, lat_max, lon_min, lon_max, buffer
        )

        # Apply upstream filtering
        filtered_ds = apply_upstream_threshold(
            ds_discharge,
            ds_upstream,
            threshold_area=GLOFAS_UPSTREAM_THRESHOLD,
            buffer=buffer,
        )

        # Convert to pandas dataframe
        filtered_df = converter.dataset_to_dataframe(
            filtered_ds["dis24"],
            cols_to_drop=["surface"],
            drop_na_subset=["dis24"],
            drop_index=False,
        )

        # Save to Parquet
        filtered_parquet_filename = f"filtered-{l_hour}.parquet"
        filtered_parquet_file_path = os.path.join(
            filtered_parquet_folder, filtered_parquet_filename
        )
        converter.dataframe_to_parquet(filtered_df, filtered_parquet_file_path)

    # Log the contents of the target folder
    context.log.info(f"Contents of {filtered_parquet_folder}:")
    context.log.info(os.listdir(filtered_parquet_folder))


@asset(
    key_prefix=["discharge"],
    deps=[transformed_discharge, rp_combined_thresh_pq],
    compute_kind="spark",
)
def forecast(context: AssetExecutionContext, pyspark: PySparkResource):
    # spark = SparkSession.builder.getOrCreate()
    spark = pyspark.spark_session
    date = datetime.utcnow() - timedelta(days=2)
    formatted_date = date.strftime("%Y-%m-%d")

    CustomSchemaWithoutTimestamp = StructType(
        [
            StructField("number", LongType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("time", LongType(), True),
            StructField("step", LongType(), True),
            StructField("valid_time", LongType(), True),
            StructField("dis24", FloatType(), True),
        ]
    )

    filtered_wildcard = "filtered-*.parquet"
    # a_couple_files = 'filtered-24*.parquet'
    # a_single_file = 'filtered-240.parquet'
    processed_discharge_filepath = os.path.join(
        DBUTILS_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date, filtered_wildcard
    )

    # Load all the forecast data from a folder into a single dataframe
    all_forecasts_df = (
        spark.read.schema(CustomSchemaWithoutTimestamp)
        .parquet(processed_discharge_filepath)
        .withColumn("latitude", F.round("latitude", GLOFAS_PRECISION))
        .withColumn("longitude", F.round("longitude", GLOFAS_PRECISION))
        .withColumn("issued_on", F.to_date(F.to_timestamp(F.col("time") / 1e9)))
        .drop("time")
        .withColumn("step", (F.col("step") / (60 * 60 * 24 * 1e9)).cast("int"))
        .withColumn("valid_time", F.to_date(F.to_timestamp(F.col("valid_time") / 1e9)))
        .withColumn("valid_for", F.date_sub("valid_time", 1))
        .drop("valid_time")
    )

    # Repartition dataframe to group by unique latitude and longitude pairs,
    # optimizing join operations on spatial coordinates.
    all_forecasts_df = all_forecasts_df.repartition(100, "latitude", "longitude")

    # Read and broadcast the threshold dataframe
    threshold_file_path = os.path.join(
        DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME
    )
    # Round the lat/lon columns as a safety measure
    # although it is assumed to already have been
    # done in the threshold joining operation
    threshold_df = (
        spark.read.parquet(threshold_file_path)
        .withColumn("latitude", F.round("latitude", GLOFAS_PRECISION))
        .withColumn("longitude", F.round("longitude", GLOFAS_PRECISION))
    )

    # Repartitioning might be better than broadcasting
    threshold_df = threshold_df.repartition(100, "latitude", "longitude")

    detailed_forecast_df = compute_flood_threshold_percentages(
        all_forecasts_df,
        threshold_df,
        GLOFAS_RET_PRD_THRESH_VALS,
        accuracy_mode="approx",
    )
    detailed_forecast_df = detailed_forecast_df.cache()

    if USE_FIRST_AS_CONTROL:
        control_df = (
            detailed_forecast_df.filter(F.col("step") == 1)
            .select("latitude", "longitude", "median_dis")
            .withColumnRenamed("median_dis", "control_dis")
        )
        print("Used first forecast as control")
    else:
        control_file_path = os.path.join(
            DBUTILS_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date, "control.parquet"
        )
        control_df = (
            spark.read.schema(CustomSchemaWithoutTimestamp)
            .parquet(control_file_path)
            .withColumn("latitude", F.round("latitude", GLOFAS_PRECISION))
            .withColumn("longitude", F.round("longitude", GLOFAS_PRECISION))
            .withColumnRenamed("dis24", "control_dis")
            .drop("step", "number", "time", "valid_time")
        )
        print("Used GloFAS control")

    # Repartitioning might be better than broadcasting
    control_df = control_df.repartition(100, "latitude", "longitude")

    # Add control discharge to the detailed forecast
    detailed_with_control_df = detailed_forecast_df.join(
        control_df, on=["latitude", "longitude"], how="left"
    )

    # Compute the summary forecast values
    tendency_df = compute_flood_tendency(
        detailed_with_control_df, GLOFAS_FLOOD_TENDENCIES, col_name="tendency"
    )
    intensity_df = compute_flood_intensity(
        detailed_forecast_df, GLOFAS_FLOOD_INTENSITIES, col_name="intensity"
    )
    peak_timing_df = compute_flood_peak_timing(
        detailed_forecast_df, GLOFAS_FLOOD_PEAK_TIMINGS, col_name="peak_timing"
    )

    # Join the three tables together to create a single summary dataframe
    tendency_and_intensity_df = tendency_df.join(
        intensity_df, on=["latitude", "longitude"]
    )
    summary_forecast_df = peak_timing_df.join(
        tendency_and_intensity_df, on=["latitude", "longitude"]
    )

    # Add the grid geometry to the forecast dataframes
    # for simple creation geometry column in geopandas
    summary_forecast_df = add_geometry(
        summary_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )
    detailed_forecast_df = add_geometry(
        detailed_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )

    # Restrict summary forecast to only the cells that
    # have a relevant flood forecast (no 'Gray' intensity)
    summary_forecast_df = summary_forecast_df.filter(
        summary_forecast_df["intensity"] != GLOFAS_FLOOD_INTENSITIES["gray"]
    )
    # Filter the detailed forecast with the idenitified
    # grid cells identified in the summary dataframe
    detailed_forecast_df = detailed_forecast_df.join(
        summary_forecast_df.select(["latitude", "longitude"]),
        on=["latitude", "longitude"],
        how="inner",
    )

    target_folder = os.path.join(S3_GLOFAS_PROCESSED_PATH, "newest")
    target_folder_db = os.path.join(DBUTILS_PREFIX, target_folder)
    target_folder_py = os.path.join(PYTHON_PREFIX, target_folder)

    # Define summary forecast file path
    summary_forecast_file_path = os.path.join(
        target_folder_db, GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME
    )
    print(summary_forecast_file_path)

    # Define detailed forecast file path
    detailed_forecast_file_path = os.path.join(
        target_folder_db, GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME
    )
    print(detailed_forecast_file_path)

    os.makedirs(target_folder_py, exist_ok=True)

    summary_forecast_df.write.mode("overwrite").parquet(summary_forecast_file_path)

    detailed_forecast_df.write.mode("overwrite").parquet(detailed_forecast_file_path)

    # Log the contents of the target folder
    context.log.info(f"Contents of {target_folder_db}:")
    context.log.info(os.listdir(target_folder_py))
