import os
from datetime import datetime, timedelta
import pandas as pd

import xarray as xr
from dagster import AssetExecutionContext, asset
import dask.dataframe as dd

from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.glofas_resource import CDSClient, CDSConfig
from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.etl.filter_by_upstream import apply_upstream_threshold
from data_pipelines.utils.flood.etl.raster_converter import RasterConverter
from data_pipelines.utils.flood.etl.transforms import (
    compute_flood_intensity,
    compute_flood_peak_timing,
    compute_flood_tendency,
    compute_flood_threshold_percentages,
    add_geometry,
)
from data_pipelines.utils.flood.etl.utils import restrict_dataset_area

from data_pipelines.partitions import discharge_partitions


def make_path(*args) -> str:
    path = os.path.join(*args)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


"""
@asset(
    key_prefix=["flood"],
    partitions_def=discharge_partitions,
    io_manager_key="grib_io_manager",
)
def raw_discharge(context: AssetExecutionContext, client: CDSClient) -> None:
    date_for_request = datetime.utcnow()  # - timedelta(days=TIMEDELTA)

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

    l_hour = context.asset_partition_key_for_output()
    target_file_path = make_path(
        OPENEPI_BASE_PATH,
        *context.asset_key.path,
        f"{l_hour}.grib",
    )
    os.makedirs(os.path.dirname(target_file_path), exist_ok=True)

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
    files = os.listdir(os.path.dirname(target_file_path))

    # Log the contents of the target folder
    context.log.info(f"Contents of {os.path.dirname(target_file_path)}:")
    context.log.info(files)
"""

"""
@asset(
    key_prefix=["flood"],
    compute_kind="xarray",
    partitions_def=discharge_partitions,
    io_manager_key="multi_partition_parquet_io_manager",
)
def transformed_discharge(
    context: AssetExecutionContext,
    raw_discharge: xr.Dataset,
    uparea_glofas_v4_0: xr.Dataset,
) -> pd.DataFrame:

    buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV
    lat_min = GLOFAS_ROI_CENTRAL_AFRICA["lat_min"]
    lat_max = GLOFAS_ROI_CENTRAL_AFRICA["lat_max"]
    lon_min = GLOFAS_ROI_CENTRAL_AFRICA["lon_min"]
    lon_max = GLOFAS_ROI_CENTRAL_AFRICA["lon_max"]

    converter = RasterConverter()

    ds_upstream = uparea_glofas_v4_0

    if USE_CONTROL_MEMBER_IN_ENSEMBLE:
        print("Combining control and ensemble")
    else:
        print("Using only ensemble")

    # Restrict discharge data to area of interest
    ds_discharge = restrict_dataset_area(
        raw_discharge, lat_min, lat_max, lon_min, lon_max, buffer
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
    return filtered_df
"""


@asset(key_prefix=["flood"])
def raw_discharge_old(context: AssetExecutionContext, client: CDSClient) -> None:
    date_for_request = datetime.utcnow()  # - timedelta(days=TIMEDELTA)
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


@asset(key_prefix=["flood"], deps=[raw_discharge_old], compute_kind="xarray")
def transformed_discharge_old(context: AssetExecutionContext):
    date_for_request = datetime.utcnow()  # - timedelta(days=TIMEDELTA)
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

    for l_hour in LEADTIME_HOURS:
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
    key_prefix=["flood"],
    deps=[transformed_discharge_old],
    compute_kind="dask",
    io_manager_key="new_parquet_io_manager",
)
def detailed_forecast_old(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    rp_combined_thresh_pq: dd.DataFrame,
):
    date_for_request = datetime.utcnow()  # - timedelta(days=TIMEDELTA)
    formatted_date = date_for_request.strftime("%Y-%m-%d")

    # Define your file path
    processed_discharge_filepath = os.path.join(
        DBUTILS_PREFIX, S3_GLOFAS_FILTERED_PATH, formatted_date, "filtered-*.parquet"
    )

    # open all filtered parquet files from the transformed_discharge asset
    forecast_df = dd.read_parquet(
        processed_discharge_filepath,
        engine="pyarrow",
        blocksize="100MB",
    )

    # Perform operations on the columns
    forecast_df["latitude"] = forecast_df["latitude"].round(GLOFAS_PRECISION)
    forecast_df["longitude"] = forecast_df["longitude"].round(GLOFAS_PRECISION)

    # Convert time and valid_time columns to date
    forecast_df["issued_on"] = forecast_df["time"].dt.date
    forecast_df["valid_for"] = (
        forecast_df["valid_time"] - pd.Timedelta(days=1)
    ).dt.date

    # Convert timedelta64 to integer representing the step
    forecast_df["step"] = (forecast_df["step"] / pd.Timedelta(days=1)).astype(int)

    # Drop unnecessary columns
    forecast_df = forecast_df.drop(columns=["time", "valid_time"])

    context.log.info(f"Joined dataframe: {forecast_df.head()}")
    context.log.info(f"Joined dataframe: {forecast_df.dtypes}")

    threshold_df = rp_combined_thresh_pq

    # Round all latitudes and longitudes to GLOFAS_PRECISION decimal places
    # threshold_df["latitude"] = threshold_df["latitude"].round(GLOFAS_PRECISION)
    # threshold_df["longitude"] = threshold_df["longitude"].round(GLOFAS_PRECISION)

    ##########################################################################################
    ############################ COMPUTE DETAILED FORECAST ###################################
    ##########################################################################################

    detailed_forecast_df = compute_flood_threshold_percentages(
        forecast_df, threshold_df
    )

    # select rows where step is equal to 1 and select only latitude, longitude,
    # and median_dis columns and rename median_dis to control_dis
    control_df = detailed_forecast_df[(detailed_forecast_df["step"] == 1)][
        ["latitude", "longitude", "median_dis"]
    ].rename(columns={"median_dis": "control_dis"})

    # merge control_df with res on latitude and longitude
    detailed_forecast_df = dd.merge(
        detailed_forecast_df, control_df, on=["latitude", "longitude"], how="left"
    )

    detailed_forecast_df = add_geometry(
        detailed_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )

    return detailed_forecast_df


"""
@asset(
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="new_parquet_io_manager",
)
def detailed_forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    transformed_discharge: dd.DataFrame,
    rp_combined_thresh_pq: dd.DataFrame,
) -> dd.DataFrame:
    # Determine the number of distinct values in the step column
    step_values = transformed_discharge["step"].unique().compute()
    context.log.info(f"STEP VALUES: {step_values}")

    forecast_df = transformed_discharge

    context.log.info(f"Joined dataframe: {forecast_df.head()}")
    context.log.info(f"Joined dataframe: {forecast_df.dtypes}")

    # log the index of forecast_df
    context.log.info(f"index: {forecast_df.index}")

    # Perform operations on the columns
    forecast_df["latitude"] = forecast_df["latitude"].round(GLOFAS_PRECISION)
    forecast_df["longitude"] = forecast_df["longitude"].round(GLOFAS_PRECISION)

    # Convert time and valid_time columns to date
    forecast_df["issued_on"] = forecast_df["time"].dt.date
    forecast_df["valid_for"] = (
        forecast_df["valid_time"] - pd.Timedelta(days=1)
    ).dt.date

    # Convert timedelta64 to integer representing the step
    forecast_df["step"] = (forecast_df["step"] / pd.Timedelta(days=1)).astype(int)

    # Drop unnecessary columns
    forecast_df = forecast_df.drop(columns=["time", "valid_time"])

    context.log.info(f"Joined dataframe: {forecast_df.head()}")
    context.log.info(f"Joined dataframe: {forecast_df.dtypes}")

    threshold_df = rp_combined_thresh_pq

    # Round all latitudes and longitudes to GLOFAS_PRECISION decimal places
    # threshold_df["latitude"] = threshold_df["latitude"].round(GLOFAS_PRECISION)
    # threshold_df["longitude"] = threshold_df["longitude"].round(GLOFAS_PRECISION)

    ##########################################################################################
    ############################ COMPUTE DETAILED FORECAST ###################################
    ##########################################################################################

    detailed_forecast_df = compute_flood_threshold_percentages(
        forecast_df, threshold_df
    )

    # select rows where step is equal to 1 and select only latitude, longitude,
    # and median_dis columns and rename median_dis to control_dis
    control_df = detailed_forecast_df[(detailed_forecast_df["step"] == 1)][
        ["latitude", "longitude", "median_dis"]
    ].rename(columns={"median_dis": "control_dis"})

    # merge control_df with res on latitude and longitude
    detailed_forecast_df = dd.merge(
        detailed_forecast_df, control_df, on=["latitude", "longitude"], how="left"
    )

    detailed_forecast_df = add_geometry(
        detailed_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )

    return detailed_forecast_df
"""

"""
@asset(
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="new_parquet_io_manager",
)
def summary_forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    detailed_forecast: dd.DataFrame,
) -> dd.DataFrame:
    detailed_forecast_df = detailed_forecast.drop(columns=["wkt"])

    ##########################################################################################
    ############################## COMPUTE PEAK TIMING #######################################
    ##########################################################################################

    peak_timing_df = compute_flood_peak_timing(detailed_forecast_df)

    ##########################################################################################
    ################################# COMPUTE TENDENCY #######################################
    ##########################################################################################

    tendency_df = compute_flood_tendency(detailed_forecast_df)

    ##########################################################################################
    ################################# COMPUTE INTENSITY ######################################
    ##########################################################################################

    intensity_df = compute_flood_intensity(detailed_forecast_df)

    ##########################################################################################
    ################################# SAVE SUMMARY FORECAST ##################################
    ##########################################################################################

    # Merge all three dataframes together
    intermediate_df = dd.merge(
        peak_timing_df, tendency_df, on=["latitude", "longitude"], how="left"
    )
    summary_forecast_df = dd.merge(
        intermediate_df, intensity_df, on=["latitude", "longitude"], how="left"
    )

    summary_forecast_df = add_geometry(
        summary_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )

    return summary_forecast_df
"""


@asset(
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="new_parquet_io_manager",
)
def summary_forecast_old(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    detailed_forecast_old: dd.DataFrame,
) -> dd.DataFrame:
    detailed_forecast_df = detailed_forecast_old.drop(columns=["wkt"])

    ##########################################################################################
    ############################## COMPUTE PEAK TIMING #######################################
    ##########################################################################################

    peak_timing_df = compute_flood_peak_timing(detailed_forecast_df)

    ##########################################################################################
    ################################# COMPUTE TENDENCY #######################################
    ##########################################################################################

    tendency_df = compute_flood_tendency(detailed_forecast_df)

    ##########################################################################################
    ################################# COMPUTE INTENSITY ######################################
    ##########################################################################################

    intensity_df = compute_flood_intensity(detailed_forecast_df)

    ##########################################################################################
    ################################# SAVE SUMMARY FORECAST ##################################
    ##########################################################################################

    # Merge all three dataframes together
    intermediate_df = dd.merge(
        peak_timing_df, tendency_df, on=["latitude", "longitude"], how="left"
    )
    summary_forecast_df = dd.merge(
        intermediate_df, intensity_df, on=["latitude", "longitude"], how="left"
    )

    summary_forecast_df = add_geometry(
        summary_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )

    return summary_forecast_df
