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
from data_pipelines.utils.flood.transforms import (
    add_geometry,
    compute_flood_intensity,
    compute_flood_peak_timing,
    compute_flood_tendency,
    compute_flood_threshold_percentages,
)
from data_pipelines.utils.flood.utils import restrict_dataset_area


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

            read_name = subarea_path.name.replace("detailed_forecast_subarea.parquet", f"split_discharge_by_area/{subarea_key}.parquet")
            write_name = subarea_path.name.replace(".parquet", f"/{subarea_key}.parquet")
            subarea_read_path = subarea_path.parent / read_name
            subarea_write_path = subarea_path.parent / write_name

            context.log.info(f"Reading from {subarea_read_path}")

            # Load the subarea discharge data
            sub_discharge = dd.read_parquet(
                subarea_read_path, storage_options=settings.base_data_upath.storage_options
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
            detailed_forecast_df = detailed_forecast_df.persist()

            detailed_forecast_df["issued_on"] = detailed_forecast_df[
                "issued_on"
            ].astype(str)
            detailed_forecast_df["valid_for"] = detailed_forecast_df[
                "valid_for"
            ].astype(str)

            detailed_forecast_df.to_parquet(
                subarea_write_path,
                overwrite=True,
                storage_options=subarea_write_path.storage_options,
            )
            context.log.info(
                f"Saved detailed forecast data to {subarea_write_path}"
            )

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

            read_name = subarea_path.name.replace("summary_forecast_subarea.parquet", f"detailed_forecast_subarea/{subarea_key}.parquet")
            write_name = subarea_path.name.replace(".parquet", f"/{subarea_key}.parquet")
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
                context.log.info(f"Skipping subarea {subarea_read_path} as it has no data")
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
