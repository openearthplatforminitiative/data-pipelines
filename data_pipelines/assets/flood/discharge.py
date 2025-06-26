import datetime as dt
from datetime import datetime, timedelta

import dask.dataframe as dd
import pandas as pd
import xarray as xr
from dagster import AssetExecutionContext, AssetIn, asset
from distributed import wait

from data_pipelines.partitions import discharge_partitions
from data_pipelines.resources import RESOURCES
from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.resources.glofas_resource import CDSClient
from data_pipelines.resources.io_managers import (
    DaskParquetIOManager,
    GribDischargeIOManager,
    get_path_in_asset,
)
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


# This is the non-partitioned version of the raw_discharge asset
# It performs sequential queries of the CDS API for each leadtime hour
# An advantage of not having a partioned asset is that the entire asset
# is materialized in the same Kubernetes pod, so it's faster than
# allocating a new pod for each partition
@asset(
    key_prefix=["flood"],
)
def raw_discharge(context: AssetExecutionContext, cds_client: CDSClient) -> None:
    date_for_request = datetime.now(dt.UTC) - timedelta(days=TIMEDELTA)

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
    else:
        product_type = ["ensemble_perturbed_forecasts"]

    for leadtime_hour in discharge_partitions.get_partition_keys():
        request_params = {
            "system_version": ["operational"],
            "hydrological_model": ["lisflood"],
            "product_type": product_type,
            "variable": "river_discharge_in_the_last_24_hours",
            "year": [str(date_for_request.year)],
            "month": ["{:02d}".format(date_for_request.month)],
            "day": ["{:02d}".format(date_for_request.day)],
            "leadtime_hour": [leadtime_hour],
            "data_format": "grib",
            "area": area,
        }

        out_path = get_path_in_asset(
            context, settings.tmp_storage, ".grib", additional_path=f"{leadtime_hour}"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        context.log.info(f"Fetching data for {request_params}")
        cds_client.fetch_data(request_params, out_path)
        context.log.info(f"Saved raw discharge data to {out_path}")

    return None


# This is the non-partitioned version of the transformed_discharge asset.
# It restricts the upstream area to the area of interest and applies the upstream threshold
# to the discharge data for each leadtime hour
# An advantage of not having a partioned asset is that the entire asset
# is materialized in the same Kubernetes pod, so it's faster than
# allocating a new pod for each partition
@asset(
    ins={
        "restricted_uparea_glofas_v4_0": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="xarray",
    deps={"raw_discharge": raw_discharge},
)
def transformed_discharge(
    context: AssetExecutionContext,
    restricted_uparea_glofas_v4_0: xr.Dataset,
) -> None:
    grib_io_manager: GribDischargeIOManager = RESOURCES["grib_io_manager"]
    parquet_io_manager: DaskParquetIOManager = RESOURCES["parquet_io_manager"]

    buffer = GLOFAS_RESOLUTION / GLOFAS_BUFFER_DIV
    lat_min = GLOFAS_ROI_CENTRAL_AFRICA["lat_min"]
    lat_max = GLOFAS_ROI_CENTRAL_AFRICA["lat_max"]
    lon_min = GLOFAS_ROI_CENTRAL_AFRICA["lon_min"]
    lon_max = GLOFAS_ROI_CENTRAL_AFRICA["lon_max"]

    ds_upstream = restricted_uparea_glofas_v4_0

    ds_upstream = restrict_dataset_area(
        ds_upstream,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        buffer,
    )

    for leadtime_hour in discharge_partitions.get_partition_keys():
        grib_path = get_path_in_asset(
            context,
            settings.tmp_storage,
            ".grib",
            additional_path=f"{leadtime_hour}",
            input_asset_key="raw_discharge",
        )

        raw_discharge = grib_io_manager.load_from_path(context, grib_path)
        context.log.info(f"Loaded raw discharge data from {grib_path}")

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
        filtered_df = dataset_to_dataframe(
            filtered_ds["dis24"],
            cols_to_drop=["surface"],
            drop_na_subset=["dis24"],
            drop_index=False,
            in_chunks=True,
        )

        # Save the filtered dataframe to a parquet file
        out_path = get_path_in_asset(
            context,
            settings.base_data_upath,
            ".parquet",
            additional_path=f"{leadtime_hour}",
        )
        parquet_io_manager.dump_to_path(context, filtered_df, out_path)
        context.log.info(f"Saved transformed discharge data to {out_path}")

    return None


# Combine split_discharge_by_area, detailed_forecast_subarea, and summary_forecast_subarea assets into a single asset
# This will ensure that the dask resource is shared across the three assets
@asset(
    ins={
        "rp_combined_thresh_pq": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    deps={"transformed_discharge": transformed_discharge},
)
def forecast(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    rp_combined_thresh_pq: dd.DataFrame,
) -> None:
    threshold_df = rp_combined_thresh_pq.persist()
    wait(threshold_df)

    parquet_io_manager: DaskParquetIOManager = RESOURCES["parquet_io_manager"]

    # define a list of paths to the transformed discharge data
    transformed_discharge_paths = [
        get_path_in_asset(
            context,
            settings.base_data_upath,
            ".parquet",
            input_asset_key="transformed_discharge",
            additional_path=f"{leadtime_hour}",
        )
        for leadtime_hour in discharge_partitions.get_partition_keys()
    ]

    transformed_discharge = parquet_io_manager.load_from_path(
        context, transformed_discharge_paths
    )
    transformed_discharge = transformed_discharge.persist()
    wait(transformed_discharge)

    # Determine the unique areas in the transformed discharge data
    total_roi = GLOFAS_ROI_CENTRAL_AFRICA
    n_subareas = N_SUBAREAS
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

            # Split the transformed discharge data into subareas

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

            # Compute detailed forecast

            subarea_key = f"tile_{i}_{j}"

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

            # convert issued_on and valid_for from date to timestamp
            # saving as date will cause issues when saving to parquet
            detailed_forecast_df["issued_on"] = detailed_forecast_df[
                "issued_on"
            ].astype("datetime64[ns]")
            detailed_forecast_df["valid_for"] = detailed_forecast_df[
                "valid_for"
            ].astype("datetime64[ns]")

            detailed_forecast_df = detailed_forecast_df.persist()
            wait(detailed_forecast_df)

            context.log.info(f"Done computing detailed forecast")

            detailed_forecast_path = get_path_in_asset(
                context,
                settings.base_data_upath,
                ".parquet",
                additional_path=subarea_key,
                replace_asset_key="detailed_forecast_subarea",
            )

            # Compute summary forecast

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

            # remove all rows with intensity equal to gray
            summary_forecast_df = summary_forecast_df[
                summary_forecast_df["intensity"] != GLOFAS_FLOOD_INTENSITIES["gray"]
            ]

            # filter the detailed forecast with the remaining
            # grid cells identified in the summary dataframe
            detailed_forecast_df = dd.merge(
                detailed_forecast_df,
                summary_forecast_df[["latitude", "longitude"]],
                on=["latitude", "longitude"],
                how="inner",
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
            wait(detailed_forecast_df)

            # Save the detailed forecast data to a parquet file
            detailed_forecast_df.to_parquet(
                detailed_forecast_path,
                overwrite=True,
                storage_options=detailed_forecast_path.storage_options,
            )
            context.log.info(
                f"Saved detailed forecast data to {detailed_forecast_path}"
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

            summary_forecast_path = get_path_in_asset(
                context,
                settings.base_data_upath,
                ".parquet",
                additional_path=subarea_key,
                replace_asset_key="summary_forecast_subarea",
            )

            # write the summary forecast data to a parquet file
            summary_forecast_df.to_parquet(
                summary_forecast_path,
                overwrite=True,
                storage_options=summary_forecast_path.storage_options,
            )
            context.log.info(f"Saved summary forecast data to {summary_forecast_path}")

    return None
