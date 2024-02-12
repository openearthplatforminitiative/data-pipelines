"""
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

import xarray as xr
from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset
from dotenv import load_dotenv
from tqdm import tqdm
import dask.dataframe as dd

from data_pipelines.assets.flood.rp_thresholds import rp_combined_thresh_pq
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.glofas_resource import CDSClient, CDSConfig
from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.etl.filter_by_upstream import apply_upstream_threshold
from data_pipelines.utils.flood.etl.raster_converter import RasterConverter
from data_pipelines.utils.flood.etl.utils import add_geometry, restrict_dataset_area


@asset(key_prefix=["flood"])
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


@asset(key_prefix=["flood"], deps=[discharge], compute_kind="xarray")
def transformed_discharge(context: AssetExecutionContext):
    date_for_request = datetime.utcnow()  # - timedelta(days=2)
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
    key_prefix=["flood"],
    deps=[transformed_discharge, rp_combined_thresh_pq],
    compute_kind="dask",
)
def forecast(context: AssetExecutionContext, dask_resource: DaskResource):
    date_for_request = datetime.utcnow()  # - timedelta(days=2)
    formatted_date = date_for_request.strftime("%Y-%m-%d")

    # Define your custom schema
    CustomSchemaWithoutTimestamp = {
        "number": int,
        "latitude": float,
        "longitude": float,
        "time": int,
        "step": int,
        "valid_time": int,
        "dis24": float,
    }

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

    context.log.info(f"Joined dataframe: {forecast_df.head()}")
    context.log.info(f"Joined dataframe: {forecast_df.dtypes}")

    # rename "time" to "issued_on"
    # forecast_df = forecast_df.rename(columns={"time": "issued_on"})

    # Perform operations on the columns
    forecast_df["latitude"] = forecast_df["latitude"].round(GLOFAS_PRECISION)
    forecast_df["longitude"] = forecast_df["longitude"].round(GLOFAS_PRECISION)
    # forecast_df["issued_on"] = pd.to_datetime(forecast_df["time"] / 1e9).dt.date

    # Convert time and valid_time columns to date
    forecast_df["issued_on"] = forecast_df["time"].dt.date
    forecast_df["valid_for"] = (
        forecast_df["valid_time"] - pd.Timedelta(days=1)
    ).dt.date

    # Convert timedelta64 to integer representing the step
    forecast_df["step"] = (forecast_df["step"] / pd.Timedelta(days=1)).astype(int)

    # forecast_df["step"] = (forecast_df["step"] / (60 * 60 * 24 * 1e9)).astype(int)
    # forecast_df["valid_time"] = pd.to_datetime(forecast_df["valid_time"] / 1e9).dt.date
    # forecast_df["valid_for"] = forecast_df["valid_time"] - pd.Timedelta(days=1)

    # Drop unnecessary columns
    forecast_df = forecast_df.drop(columns=["time", "valid_time"])

    # round all latitudes and longitudes to GLOFAS_PRECISION decimal places
    # forecast_df['latitude'] = forecast_df['latitude'].round(GLOFAS_PRECISION)
    # forecast_df['longitude'] = forecast_df['longitude'].round(GLOFAS_PRECISION)

    context.log.info(f"Joined dataframe: {forecast_df.head()}")
    context.log.info(f"Joined dataframe: {forecast_df.dtypes}")

    # open threshold parquet dataset from public s3 bucket with pandas and dask
    threshold_filepath = os.path.join(
        DBUTILS_PREFIX, S3_GLOFAS_AUX_DATA_PATH, GLOFAS_PROCESSED_THRESH_FILENAME
    )
    threshold_df = dd.read_parquet(threshold_filepath, engine="pyarrow", chunks="auto")

    threshold_cols = [
        f"threshold_{int(threshold)}y" for threshold in GLOFAS_RET_PRD_THRESH_VALS
    ]

    # Round all latitudes and longitudes to GLOFAS_PRECISION decimal places
    threshold_df["latitude"] = threshold_df["latitude"].round(GLOFAS_PRECISION)
    threshold_df["longitude"] = threshold_df["longitude"].round(GLOFAS_PRECISION)

    # Merge forecast dataframe with threshold dataframe on latitude and longitude
    joined_ddf = dd.merge(
        forecast_df, threshold_df, on=["latitude", "longitude"], how="left"
    )

    # Create columns for exceedance
    for threshold, col_name in zip(GLOFAS_RET_PRD_THRESH_VALS, threshold_cols):
        exceed_col = f"exceed_{int(threshold)}y"
        joined_ddf[exceed_col] = (joined_ddf["dis24"] >= joined_ddf[col_name]).astype(
            "int64"
        )

    q1_fun = dd.Aggregation(
        name="q1", chunk=lambda s: s.quantile(0.25), agg=lambda s0: s0.quantile(0.25)
    )

    median_fun = dd.Aggregation(
        name="median", chunk=lambda s: s.median(), agg=lambda s0: s0.sum()
    )

    q3_fun = dd.Aggregation(
        name="q3", chunk=lambda s: s.quantile(0.75), agg=lambda s0: s0.quantile(0.75)
    )

    res = (
        joined_ddf.groupby(["latitude", "longitude", "issued_on", "valid_for", "step"])
        .agg(
            min_dis24=("dis24", "min"),
            q1_dis24=("dis24", q1_fun),
            median_dis24=("dis24", median_fun),
            q3_dis24=("dis24", q3_fun),
            max_dis24=("dis24", "max"),
            p_above_2y=("exceed_2y", "mean"),
            p_above_5y=("exceed_5y", "mean"),
            p_above_20y=("exceed_20y", "mean"),
            numeric_only=pd.NamedAgg("dis24", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    # select rows where step is equal to 1 and select only latitude, longitude, and median_dis24 columns
    # and rename median_dis24 to control_dis
    control_df = res[(res["step"] == 1)][
        ["latitude", "longitude", "median_dis24"]
    ].rename(columns={"median_dis24": "control_dis"})

    # merge control_df with res on latitude and longitude
    detailed_forecast_df = dd.merge(
        res, control_df, on=["latitude", "longitude"], how="left"
    )

    ##########################################################################################

    df_for_timing = detailed_forecast_df.drop(
        columns=["min_dis24", "q1_dis24", "q3_dis24", "max_dis24", "control_dis"]
    )
    col_name = "peak_timing"

    # flood_peak_timings = {
    #    'black_border': 1,
    #    'grayed_color': 2,
    #    'gray_border': 3
    # }

    # 1. Filter rows between steps 1 to 10
    filtered_ddf = df_for_timing[
        (df_for_timing["step"] >= 1) & (df_for_timing["step"] <= 10)
    ]  # change to 10

    # 2. Compute the maximum flood probability above the 2-year return period threshold for the first ten days
    max_ddf = (
        filtered_ddf.groupby(["latitude", "longitude"])
        .agg({"p_above_2y": "max"})
        .rename(columns={"p_above_2y": "max_2y_start"})
        .reset_index()
    )

    # 3. Join the max probabilities back to the main DataFrame
    df = dd.merge(df_for_timing, max_ddf, on=["latitude", "longitude"], how="left")

    def condition_func(df):
        df["condition"] = np.where(
            df["p_above_20y"] >= 0.3,
            4,
            np.where(
                df["p_above_5y"] >= 0.3, 3, np.where(df["p_above_2y"] >= 0.3, 2, 1)
            ),
        )
        return df

    ddf_conds = df.map_partitions(condition_func)

    ddf_conds = ddf_conds.drop(columns=["p_above_2y", "p_above_5y", "p_above_20y"])

    def sort_and_select_first_row(df):
        sorted_df = df.sort_values(
            ["latitude", "longitude", "condition", "median_dis24"], ascending=False
        )
        first_row_df = (
            sorted_df.groupby(["latitude", "longitude"]).first().reset_index()
        )
        return first_row_df

    ddf_maps = ddf_conds.map_partitions(sort_and_select_first_row, meta=ddf_conds).drop(
        columns=["median_dis24"]
    )

    # Rename step column to peak_step
    ddf_maps = ddf_maps.rename(columns={"step": "peak_step", "valid_for": "peak_day"})

    # def peak_timing_func(df):
    #    df['peak_timing'] = df.apply(lambda row: flood_peak_timings['black_border'] if row['peak_step'] in range(1, 4) else
    #                                           (flood_peak_timings['grayed_color'] if row['peak_step'] > 4 and row['max_2y_start'] < 0.30 else
    #                                            flood_peak_timings['gray_border']), axis=1)
    #    return df

    def peak_timing_func(df):
        df["peak_timing"] = np.where(
            (df["peak_step"].isin(range(1, 10))) & (df["max_2y_start"] >= 0.30),
            GLOFAS_FLOOD_PEAK_TIMINGS["black_border"],
            np.where(
                (df["peak_step"] > 10) & (df["max_2y_start"] < 0.30),
                GLOFAS_FLOOD_PEAK_TIMINGS["grayed_color"],
                GLOFAS_FLOOD_PEAK_TIMINGS["gray_border"],
            ),
        )
        return df

    ddf = ddf_maps.map_partitions(peak_timing_func).drop(
        columns=["condition", "max_2y_start"]
    )

    ##########################################################################################

    # flood_tendencies = {
    #    'increasing': 1,
    #    'decreasing': 2,
    #    'stagnant': 3
    # }

    df_for_tendency = detailed_forecast_df.drop(
        columns=["q1_dis24", "q3_dis24", "p_above_2y", "p_above_5y", "p_above_20y"]
    )

    grid_cell_tendency = (
        df_for_tendency.groupby(["latitude", "longitude"])
        .agg(
            max_median_dis=("median_dis24", "max"),
            min_median_dis=("median_dis24", "min"),
            control_dis=("control_dis", "first"),
            max_max_dis=("max_dis24", "max"),
            min_min_dis=("min_dis24", "min"),
            numeric_only=pd.NamedAgg("max_dis24", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    def tendency_func(df):
        df["tendency"] = np.where(
            df["max_median_dis"] > df["control_dis"] * 1.10,
            GLOFAS_FLOOD_TENDENCIES["increasing"],
            np.where(
                (df["min_median_dis"] <= df["control_dis"] * 0.90)
                & (df["max_median_dis"] <= df["control_dis"] * 1.10),
                GLOFAS_FLOOD_TENDENCIES["decreasing"],
                GLOFAS_FLOOD_TENDENCIES["stagnant"],
            ),
        )
        return df

    grid_cell_tendency = grid_cell_tendency.map_partitions(tendency_func)

    ##########################################################################################

    # flood_intensities = {
    #    'purple': 1,
    #    'red': 2,
    #    'yellow': 3,
    #    'gray': 4
    # }

    df_for_intensity = detailed_forecast_df.drop(
        columns=["min_dis24", "q1_dis24", "q3_dis24", "max_dis24", "control_dis"]
    )

    grid_cell_intensity = (
        df_for_intensity.groupby(["latitude", "longitude"])
        .agg(
            max_p_above_20y=("p_above_20y", "max"),
            max_p_above_5y=("p_above_5y", "max"),
            max_p_above_2y=("p_above_2y", "max"),
            numeric_only=pd.NamedAgg("p_above_20y", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    def intensity_func(df):
        df["intensity"] = np.where(
            df["max_p_above_20y"] >= 0.30,
            GLOFAS_FLOOD_INTENSITIES["purple"],
            np.where(
                df["max_p_above_5y"] >= 0.30,
                GLOFAS_FLOOD_INTENSITIES["red"],
                np.where(
                    df["max_p_above_2y"] >= 0.30,
                    GLOFAS_FLOOD_INTENSITIES["yellow"],
                    GLOFAS_FLOOD_INTENSITIES["gray"],
                ),
            ),
        )
        return df

    grid_cell_intensity = grid_cell_intensity.map_partitions(intensity_func)

    ##########################################################################################

    # Merge all three dataframes together
    final_df = dd.merge(
        ddf, grid_cell_tendency, on=["latitude", "longitude"], how="left"
    )
    summary_forecast_df = dd.merge(
        final_df, grid_cell_intensity, on=["latitude", "longitude"], how="left"
    )

    # Add the grid geometry to the forecast dataframes
    # for simple creation geometry column in geopandas
    summary_forecast_df = add_geometry(
        summary_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )
    detailed_forecast_df = add_geometry(
        detailed_forecast_df, GLOFAS_RESOLUTION / 2, GLOFAS_PRECISION
    )

    target_folder = os.path.join(S3_GLOFAS_PROCESSED_PATH, "newest")
    target_folder_db = os.path.join(DBUTILS_PREFIX, target_folder)

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

    os.makedirs(target_folder_db, exist_ok=True)

    # Overwrite the detailed and summary dataframes to parquet
    summary_forecast_df.to_parquet(
        summary_forecast_file_path, write_index=False, overwrite=True
    )
    detailed_forecast_df.to_parquet(
        detailed_forecast_file_path, write_index=False, overwrite=True
    )

    # Log the contents of the target folder
    context.log.info(f"Contents of {target_folder_db}:")
    context.log.info(os.listdir(target_folder_db))
"""
