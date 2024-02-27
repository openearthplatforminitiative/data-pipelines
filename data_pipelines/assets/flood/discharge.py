import os
from datetime import datetime, timedelta
import pandas as pd

import xarray as xr
from dagster import AssetExecutionContext, asset
import dask.dataframe as dd
from data_pipelines.settings import settings

from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.glofas_resource import CDSClient
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


# This is the partitioned version of the raw_discharge asset
# It performs prallelized queries of the CDS API for each leadtime hour
# Such parallelization can cause the pipeline to fail with no clear error message
# Using a tag for limiting concurrency can help prevent this
@asset(
    key_prefix=["flood"],
    partitions_def=discharge_partitions,
    io_manager_key="grib_io_manager",
)
def raw_discharge(context: AssetExecutionContext, client: CDSClient) -> None:
    date_for_request = datetime.utcnow() - timedelta(days=TIMEDELTA)

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

    leadtime_hour = context.partition_key
    target_file_path = make_path(
        settings.base_data_path,
        *context.asset_key.path,
        f"{leadtime_hour}.grib",
    )
    os.makedirs(os.path.dirname(target_file_path), exist_ok=True)

    request_params = {
        "system_version": "operational",
        "hydrological_model": "lisflood",
        "variable": "river_discharge_in_the_last_24_hours",
        "format": "grib",
        "year": date_for_request.year,
        "month": date_for_request.month,
        "day": date_for_request.day,
        "leadtime_hour": leadtime_hour,
        "area": area,
        "product_type": product_type,
    }

    # Fetch the data
    client.fetch_data(request_params, target_file_path)

    # get the list of files in the folder
    files = os.listdir(os.path.dirname(target_file_path))

    # Log the contents of the target folder
    context.log.info(f"Contents of {os.path.dirname(target_file_path)}:")
    context.log.info(files)


@asset(
    key_prefix=["flood"],
    compute_kind="xarray",
    partitions_def=discharge_partitions,
    io_manager_key="parquet_io_manager",
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
        raw_discharge,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        buffer,
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


@asset(
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="parquet_io_manager",
)
def detailed_forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    transformed_discharge: dd.DataFrame,
    rp_combined_thresh_pq: dd.DataFrame,
) -> dd.DataFrame:
    forecast_df = transformed_discharge

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

    threshold_df = rp_combined_thresh_pq

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


@asset(
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="parquet_io_manager",
)
def summary_forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    detailed_forecast: dd.DataFrame,
) -> dd.DataFrame:
    detailed_forecast_df = detailed_forecast.drop(columns=["wkt"])

    peak_timing_df = compute_flood_peak_timing(detailed_forecast_df)
    tendency_df = compute_flood_tendency(detailed_forecast_df)
    intensity_df = compute_flood_intensity(detailed_forecast_df)

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
