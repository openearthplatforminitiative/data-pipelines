import time
from datetime import datetime, timedelta

import dask.dataframe as dd
import pandas as pd
import xarray as xr
from dagster import AssetExecutionContext, AssetIn, asset
from distributed import futures_of, wait

from data_pipelines.partitions import discharge_partitions
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.resources.io_managers import get_path_in_asset
from data_pipelines.settings import settings
from data_pipelines.utils.flood.config import *
from data_pipelines.utils.flood.filter_by_upstream import apply_upstream_threshold
from data_pipelines.utils.flood.raster_converter import dataset_to_dataframe

# from data_pipelines.utils.flood.transforms import (
#     add_geometry,
#     compute_flood_intensity,
#     compute_flood_peak_timing,
#     compute_flood_tendency,
#     compute_flood_threshold_percentages,
# )
from data_pipelines.utils.flood.utils import restrict_dataset_area


def compute_flood_threshold_percentages(
    forecast_df: dd.DataFrame,
    threshold_df: dd.DataFrame,
    ret_period_vals: list[int] = GLOFAS_RET_PRD_THRESH_VALS,
) -> dd.DataFrame:
    """
    Compute the flood threshold percentages for the forecast DataFrame.

    - Args:
    - forecast_df (dd.DataFrame): The forecast DataFrame.
    - threshold_df (dd.DataFrame): The threshold DataFrame.
    - ret_period_vals (list[int]): The return period values.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the threshold percentages.
    """
    # Merge forecast dataframe with threshold dataframe on latitude and longitude
    joined_ddf = dd.merge(
        forecast_df, threshold_df, on=["latitude", "longitude"], how="left"
    )

    threshold_cols = [f"threshold_{int(threshold)}y" for threshold in ret_period_vals]

    # Create columns for exceedance
    for threshold, col_name in zip(ret_period_vals, threshold_cols):
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

    detailed_forecast_df = (
        joined_ddf.groupby(["latitude", "longitude", "issued_on", "valid_for", "step"])
        .agg(
            min_dis=("dis24", "min"),
            q1_dis=("dis24", q1_fun),
            median_dis=("dis24", median_fun),
            q3_dis=("dis24", q3_fun),
            max_dis=("dis24", "max"),
            p_above_2y=("exceed_2y", "mean"),
            p_above_5y=("exceed_5y", "mean"),
            p_above_20y=("exceed_20y", "mean"),
            numeric_only=pd.NamedAgg("dis24", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    return detailed_forecast_df


def compute_flood_peak_timing(
    detailed_forecast_df: dd.DataFrame,
    col_name: str = "peak_timing",
    peak_timings: dict[str, str] = GLOFAS_FLOOD_PEAK_TIMINGS,
) -> dd.DataFrame:
    """
    Compute the flood peak timing for the forecast DataFrame.

    - Args:
    - detailed_forecast_df (dd.DataFrame): The detailed forecast DataFrame.
    - col_name (str): The name of the column to add.
    - peak_timings (dict[str, str]): The mapping from peak timings to border colors.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the peak timings.
    """
    df_for_timing = detailed_forecast_df.drop(
        columns=["min_dis", "q1_dis", "q3_dis", "max_dis", "control_dis"]
    )

    # 1. Filter rows between steps 1 to 10
    filtered_ddf = df_for_timing[
        (df_for_timing["step"] >= 1) & (df_for_timing["step"] <= 10)
    ]

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
            ["latitude", "longitude", "condition", "median_dis"], ascending=False
        )
        first_row_df = (
            sorted_df.groupby(["latitude", "longitude"]).first().reset_index()
        )
        return first_row_df

    ddf_maps = ddf_conds.map_partitions(sort_and_select_first_row, meta=ddf_conds).drop(
        columns=["median_dis"]
    )

    # Rename step column to peak_step
    ddf_maps = ddf_maps.rename(columns={"step": "peak_step", "valid_for": "peak_day"})

    def peak_timing_func(df):
        df[col_name] = np.where(
            (df["peak_step"].isin(range(1, 4))) & (df["max_2y_start"] >= 0.30),
            peak_timings["black_border"],
            np.where(
                (df["peak_step"] > 10) & (df["max_2y_start"] < 0.30),
                peak_timings["grayed_color"],
                peak_timings["gray_border"],
            ),
        )
        return df

    ddf = ddf_maps.map_partitions(peak_timing_func).drop(
        columns=["condition", "max_2y_start"]
    )

    return ddf


def compute_flood_tendency(
    detailed_forecast_df: dd.DataFrame,
    col_name: str = "tendency",
    tendencies: dict[str, str] = GLOFAS_FLOOD_TENDENCIES,
) -> dd.DataFrame:
    """
    Compute the flood tendency for the forecast DataFrame.

    - Args:
    - detailed_forecast_df (dd.DataFrame): The detailed forecast DataFrame.
    - col_name (str): The name of the column to add.
    - tendencies (dict[str, str]): The mapping from tendencies to shapes.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the tendencies.
    """
    df_for_tendency = detailed_forecast_df.drop(
        columns=["q1_dis", "q3_dis", "p_above_2y", "p_above_5y", "p_above_20y"]
    )

    grid_cell_tendency = (
        df_for_tendency.groupby(["latitude", "longitude"])
        .agg(
            max_median_dis=("median_dis", "max"),
            min_median_dis=("median_dis", "min"),
            control_dis=("control_dis", "first"),
            max_max_dis=("max_dis", "max"),
            min_min_dis=("min_dis", "min"),
            numeric_only=pd.NamedAgg("max_dis", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    def tendency_func(df):
        df[col_name] = np.where(
            df["max_median_dis"] > df["control_dis"] * 1.10,
            tendencies["increasing"],
            np.where(
                (df["min_median_dis"] <= df["control_dis"] * 0.90)
                & (df["max_median_dis"] <= df["control_dis"] * 1.10),
                tendencies["decreasing"],
                tendencies["stagnant"],
            ),
        )
        return df

    grid_cell_tendency = grid_cell_tendency.map_partitions(tendency_func)

    return grid_cell_tendency


def compute_flood_intensity(
    detailed_forecast_df: dd.DataFrame,
    col_name: str = "intensity",
    intensities: dict[str, str] = GLOFAS_FLOOD_INTENSITIES,
) -> dd.DataFrame:
    """
    Compute the flood intensity for the forecast DataFrame.

    - Args:
    - detailed_forecast_df (dd.DataFrame): The detailed forecast DataFrame.
    - col_name (str): The name of the column to add.
    - intensities (dict[str, str]): The mapping from flood intensities to colors.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the intensities.
    """
    df_for_intensity = detailed_forecast_df.drop(
        columns=["min_dis", "q1_dis", "q3_dis", "max_dis", "control_dis"]
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
        df[col_name] = np.where(
            df["max_p_above_20y"] >= 0.30,
            intensities["purple"],
            np.where(
                df["max_p_above_5y"] >= 0.30,
                intensities["red"],
                np.where(
                    df["max_p_above_2y"] >= 0.30,
                    intensities["yellow"],
                    intensities["gray"],
                ),
            ),
        )
        return df

    grid_cell_intensity = grid_cell_intensity.map_partitions(intensity_func)

    return grid_cell_intensity


def add_geometry(
    df: pd.DataFrame | dd.core.DataFrame, half_grid_size: float, precision: int
) -> pd.DataFrame | dd.core.DataFrame:
    """
    Add a geometry column to the DataFrame.

    - Args:
    - df (pd.DataFrame | dd.core.DataFrame): The DataFrame.
    - half_grid_size (float): The half grid size.
    - precision (int): The precision.

    - Returns:
    pd.DataFrame | dd.core.DataFrame: The DataFrame with the geometry column.
    """
    df["min_latitude"] = (df["latitude"] - half_grid_size).round(precision)
    df["max_latitude"] = (df["latitude"] + half_grid_size).round(precision)
    df["min_longitude"] = (df["longitude"] - half_grid_size).round(precision)
    df["max_longitude"] = (df["longitude"] + half_grid_size).round(precision)

    df["wkt"] = (
        "POLYGON (("
        + df["min_longitude"].astype(str)
        + " "
        + df["min_latitude"].astype(str)
        + ","
        + df["min_longitude"].astype(str)
        + " "
        + df["max_latitude"].astype(str)
        + ","
        + df["max_longitude"].astype(str)
        + " "
        + df["max_latitude"].astype(str)
        + ","
        + df["max_longitude"].astype(str)
        + " "
        + df["min_latitude"].astype(str)
        + ","
        + df["min_longitude"].astype(str)
        + " "
        + df["min_latitude"].astype(str)
        + "))"
    )

    df = df.drop(
        ["min_latitude", "max_latitude", "min_longitude", "max_longitude"], axis=1
    )

    return df


# This is the partitioned version of the raw_discharge asset
# It performs prallelized queries of the CDS API for each leadtime hour
# Such parallelization can cause the pipeline to fail with no clear error message
# Using a tag for limiting concurrency can help prevent this
@asset(
    key_prefix=["flood"],
    partitions_def=discharge_partitions,
    io_manager_key="grib_io_manager",
)
def raw_discharge(context: AssetExecutionContext, cds_client: CDSClient) -> None:
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

    request_params = {
        "system_version": "operational",
        "hydrological_model": "lisflood",
        "variable": "river_discharge_in_the_last_24_hours",
        "format": "grib",
        "year": date_for_request.year,
        "month": date_for_request.month,
        "day": date_for_request.day,
        "leadtime_hour": context.partition_key,
        "area": area,
        "product_type": product_type,
    }

    out_path = get_path_in_asset(context, settings.tmp_storage, ".grib")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(f"Fetching data for {request_params}")
    cds_client.fetch_data(request_params, out_path)


@asset(
    ins={
        "raw_discharge": AssetIn(key_prefix="flood"),
        "restricted_uparea_glofas_v4_0": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="xarray",
    partitions_def=discharge_partitions,
    io_manager_key="parquet_io_manager",
)
def transformed_discharge(
    context: AssetExecutionContext,
    raw_discharge: xr.Dataset,
    restricted_uparea_glofas_v4_0: xr.Dataset,
) -> pd.DataFrame:
    buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV
    lat_min = GLOFAS_ROI_CENTRAL_AFRICA["lat_min"]
    lat_max = GLOFAS_ROI_CENTRAL_AFRICA["lat_max"]
    lon_min = GLOFAS_ROI_CENTRAL_AFRICA["lon_min"]
    lon_max = GLOFAS_ROI_CENTRAL_AFRICA["lon_max"]

    ds_upstream = restricted_uparea_glofas_v4_0

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

    ds_upstream = restrict_dataset_area(
        ds_upstream,
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
    filtered_df = dataset_to_dataframe(
        filtered_ds["dis24"],
        cols_to_drop=["surface"],
        drop_na_subset=["dis24"],
        drop_index=False,
        in_chunks=True,
    )
    return filtered_df


# Define a new asset that will open the transformed_discharge asset and split it up based on separate areas
# This asset intends to partition the transformed_discharge asset into separate areas that can be processed separately
@asset(
    ins={
        "transformed_discharge": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="dummy_io_manager",
)
def split_discharge_by_area(
    context: AssetExecutionContext,
    transformed_discharge: dd.DataFrame,
    dask_resource: DaskResource,
) -> None:
    # Load the transformed discharge data
    transformed_discharge = transformed_discharge.persist()
    wait(transformed_discharge)

    # Determine the unique areas in the transformed discharge data
    total_roi = GLOFAS_ROI_CENTRAL_AFRICA
    n_subareas = 4
    lat_min = total_roi["lat_min"]
    lat_max = total_roi["lat_max"]
    lon_min = total_roi["lon_min"]
    lon_max = total_roi["lon_max"]

    for i in range(n_subareas):
        lat_min_sub = lat_min + i * (lat_max - lat_min) / n_subareas
        lat_max_sub = lat_min + (i + 1) * (lat_max - lat_min) / n_subareas

        for j in range(n_subareas):
            lon_min_sub = lon_min + j * (lon_max - lon_min) / n_subareas
            lon_max_sub = lon_min + (j + 1) * (lon_max - lon_min) / n_subareas

            sub_discharge = transformed_discharge[
                (transformed_discharge["latitude"] >= lat_min_sub)
                & (transformed_discharge["latitude"] < lat_max_sub)
                & (transformed_discharge["longitude"] >= lon_min_sub)
                & (transformed_discharge["longitude"] < lon_max_sub)
            ]

            # Persist the subarea discharge data
            sub_discharge = sub_discharge.persist()
            wait(sub_discharge)

            # find min and max lat and lon of the subarea
            lat_min_sub_true = sub_discharge["latitude"].min().compute()
            lat_max_sub_true = sub_discharge["latitude"].max().compute()
            lon_min_sub_true = sub_discharge["longitude"].min().compute()
            lon_max_sub_true = sub_discharge["longitude"].max().compute()

            # print out the min and max lat and lon of the subarea
            context.log.info(
                f"Subarea {lat_min_sub_true} to {lat_max_sub_true} and {lon_min_sub_true} to {lon_max_sub_true}"
            )

            # write the subarea discharge data to a parquet file
            subarea_key = f"tile_{i}_{j}"

            subarea_path = get_path_in_asset(
                context,
                settings.base_data_upath,
                ".parquet",
                additional_path=subarea_key,
            )
            sub_discharge.to_parquet(
                subarea_path,
                overwrite=True,
                storage_options=subarea_path.storage_options,
            )
            context.log.info(f"Saved subarea discharge data to {subarea_path}")

    return None


# Create a new detailed forecast asset that will open all the subarea discharge data separately and compute the detailed forecast
# This asset intends to compute the detailed forecast for each subarea in sequence
@asset(
    ins={
        "rp_combined_thresh_pq": AssetIn(key_prefix="flood"),
        "split_discharge_by_area": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="dummy_io_manager",
)
def detailed_forecast_subarea(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    rp_combined_thresh_pq: dd.DataFrame,
    split_discharge_by_area: int,
) -> None:
    threshold_df = rp_combined_thresh_pq.persist()
    wait(threshold_df)

    for i in range(N_SUBAREAS):
        for j in range(N_SUBAREAS):
            subarea_key = f"tile_{i}_{j}"

            subarea_path = get_path_in_asset(
                context,
                settings.base_data_upath,
                ".parquet",
            )

            read_name = subarea_path.name.replace(
                "detailed_forecast_subarea.parquet",
                f"split_discharge_by_area/{subarea_key}.parquet",
            )
            write_name = subarea_path.name.replace(
                ".parquet", f"/{subarea_key}.parquet"
            )
            subarea_read_path = subarea_path.parent / read_name
            subarea_write_path = subarea_path.parent / write_name

            context.log.info(f"Reading from {subarea_read_path}")

            # Load the subarea discharge data
            sub_discharge = dd.read_parquet(
                subarea_read_path,
                storage_options=settings.base_data_upath.storage_options,
            )

            # If the subarea discharge data has no rows, skip the computation
            if sub_discharge.shape[0].compute() == 0:
                context.log.info(f"Skipping subarea {subarea_key} as it has no data")
                continue

            # Perform operations on the columns
            sub_discharge["latitude"] = sub_discharge["latitude"].round(
                GLOFAS_PRECISION
            )
            sub_discharge["longitude"] = sub_discharge["longitude"].round(
                GLOFAS_PRECISION
            )

            # Convert time and valid_time columns to date
            sub_discharge["issued_on"] = sub_discharge["time"].dt.date
            sub_discharge["valid_for"] = (
                sub_discharge["valid_time"] - pd.Timedelta(days=1)
            ).dt.date

            # Convert timedelta64 to integer representing the step
            sub_discharge["step"] = (
                sub_discharge["step"] / pd.Timedelta(days=1)
            ).astype(int)

            # Drop unnecessary columns
            sub_discharge = sub_discharge.drop(columns=["time", "valid_time"])

            detailed_forecast_df = compute_flood_threshold_percentages(
                sub_discharge, threshold_df
            )

            # select rows where step is equal to 1 and select only latitude, longitude,
            # and median_dis columns and rename median_dis to control_dis
            control_df = detailed_forecast_df[(detailed_forecast_df["step"] == 1)][
                ["latitude", "longitude", "median_dis"]
            ].rename(columns={"median_dis": "control_dis"})

            # merge control_df with res on latitude and longitude
            detailed_forecast_df = dd.merge(
                detailed_forecast_df,
                control_df,
                on=["latitude", "longitude"],
                how="left",
            )

            new_meta = detailed_forecast_df._meta.copy()
            new_meta["wkt"] = "str"

            # Apply this optimized function
            detailed_forecast_df = detailed_forecast_df.map_partitions(
                add_geometry,
                half_grid_size=GLOFAS_RESOLUTION / 2,
                precision=GLOFAS_PRECISION,
                meta=new_meta,
            )

            # detailed_forecast_df = add_geometry(
            #     detailed_forecast_df,
            #     half_grid_size=GLOFAS_RESOLUTION / 2,
            #     precision=GLOFAS_PRECISION,
            # )

            detailed_forecast_df["issued_on"] = detailed_forecast_df[
                "issued_on"
            ].astype(str)
            detailed_forecast_df["valid_for"] = detailed_forecast_df[
                "valid_for"
            ].astype(str)

            detailed_forecast_df = detailed_forecast_df.persist()
            wait(detailed_forecast_df)

            context.log.info(f"Done computing detailed forecast")

            detailed_forecast_df.to_parquet(
                subarea_write_path,
                overwrite=True,
                storage_options=subarea_write_path.storage_options,
            )
            context.log.info(f"Saved detailed forecast data to {subarea_write_path}")

            del sub_discharge
            del detailed_forecast_df

    return None


@asset(
    ins={
        "transformed_discharge": AssetIn(key_prefix="flood"),
        "rp_combined_thresh_pq": AssetIn(key_prefix="flood"),
    },
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
    client = dask_resource._client
    forecast_df = transformed_discharge.persist()
    wait(forecast_df)
    context.log.info(f"Has what: {client.has_what()}")

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

    threshold_df = rp_combined_thresh_pq.persist()
    wait(threshold_df)
    context.log.info(f"Has what: {client.has_what()}")

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

    new_meta = detailed_forecast_df._meta.copy()
    new_meta["wkt"] = "str"

    detailed_forecast_df = detailed_forecast_df.map_partitions(
        add_geometry,
        half_grid_size=GLOFAS_RESOLUTION / 2,
        precision=GLOFAS_PRECISION,
        meta=new_meta,
    )

    detailed_forecast_df["issued_on"] = detailed_forecast_df["issued_on"].astype(str)
    detailed_forecast_df["valid_for"] = detailed_forecast_df["valid_for"].astype(str)

    context.log.info(f"Started computing detailed forecast")
    context.log.info(f"Has what: {client.has_what()}")
    detailed_forecast_df = detailed_forecast_df.persist()
    wait(detailed_forecast_df)
    context.log.info(f"Has what: {client.has_what()}")
    context.log.info(f"Finished computing detailed forecast")

    return detailed_forecast_df


@asset(
    ins={
        "transformed_discharge": AssetIn(key_prefix="flood"),
        "rp_combined_thresh_pq": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="parquet_io_manager",
)
def dummy_detailed_forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    transformed_discharge: dd.DataFrame,
    rp_combined_thresh_pq: dd.DataFrame,
) -> dd.DataFrame:
    client = dask_resource._client
    # forecast_df = client.persist(transformed_discharge)
    forecast_df = transformed_discharge.persist()
    wait(forecast_df)

    # Perform operations on the columns
    forecast_df["latitude"] = forecast_df["latitude"].round(GLOFAS_PRECISION)
    forecast_df["longitude"] = forecast_df["longitude"].round(GLOFAS_PRECISION)

    context.log.info(f"Has what: {client.has_what()}")
    context.log.info(f"Who has: {client.who_has()}")
    context.log.info(f"Who has futures of: {client.who_has(futures_of(forecast_df))}")
    context.log.info(f"Finished computing detailed forecast")

    detailed_forecast_df = forecast_df.persist()
    # detailed_forecast_df = client.persist(forecast_df)
    wait(detailed_forecast_df)

    context.log.info(f"Has what: {client.has_what()}")
    context.log.info(f"Who has: {client.who_has()}")
    context.log.info(
        f"Who has futures of: {client.who_has(futures_of(detailed_forecast_df))}"
    )
    context.log.info(f"Finished computing detailed forecast")

    time.sleep(30)

    return detailed_forecast_df


@asset(
    ins={
        "detailed_forecast": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="parquet_io_manager",
)
def summary_forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    detailed_forecast: dd.DataFrame,
) -> dd.DataFrame:
    client = dask_resource._client
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

    context.log.info(f"Started computing summary forecast")
    summary_forecast_df = client.compute(summary_forecast_df).result()
    context.log.info(f"Finished computing summary forecast")

    return summary_forecast_df


# This asset will open the detailed forecast data for each subarea and compute the summary forecast sequentially
@asset(
    ins={
        "detailed_forecast_subarea": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="dummy_io_manager",
)
def summary_forecast_subarea(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    detailed_forecast_subarea: int,
) -> None:
    for i in range(N_SUBAREAS):
        for j in range(N_SUBAREAS):
            subarea_key = f"tile_{i}_{j}"
            subarea_path = get_path_in_asset(
                context,
                settings.base_data_upath,
                ".parquet",
            )

            read_name = subarea_path.name.replace(
                "summary_forecast_subarea.parquet",
                f"detailed_forecast_subarea/{subarea_key}.parquet",
            )
            write_name = subarea_path.name.replace(
                ".parquet", f"/{subarea_key}.parquet"
            )
            subarea_read_path = subarea_path.parent / read_name
            subarea_write_path = subarea_path.parent / write_name

            # Load the detailed forecast data if it exists
            if subarea_read_path.exists():
                context.log.info(f"Reading from {subarea_read_path}")
                detailed_forecast_df = dd.read_parquet(
                    subarea_read_path,
                    storage_options=settings.base_data_upath.storage_options,
                )
            else:
                context.log.info(
                    f"Skipping subarea {subarea_read_path} as it has no data"
                )
                continue

            detailed_forecast_df = detailed_forecast_df.drop(columns=["wkt"])

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

            # add geometry to the summary forecast dataframe
            new_meta = summary_forecast_df._meta.copy()
            new_meta["wkt"] = "str"
            summary_forecast_df = summary_forecast_df.map_partitions(
                add_geometry,
                half_grid_size=GLOFAS_RESOLUTION / 2,
                precision=GLOFAS_PRECISION,
                meta=new_meta,
            )

            summary_forecast_df = summary_forecast_df.persist()
            wait(summary_forecast_df)

            context.log.info("Done computing summary forecast")

            # write the summary forecast data to a parquet file
            summary_forecast_df.to_parquet(
                subarea_write_path,
                overwrite=True,
                storage_options=subarea_write_path.storage_options,
            )
            context.log.info(f"Saved summary forecast data to {subarea_write_path}")

            # delete the detailed forecast variable
            del detailed_forecast_df
            del summary_forecast_df
            del peak_timing_df
            del tendency_df
            del intensity_df
            del intermediate_df

    return None
