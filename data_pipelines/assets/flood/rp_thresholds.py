import xarray as xr
from dagster import AssetExecutionContext, AssetIn, asset

from data_pipelines.resources.dask_resource import DaskResource
from data_pipelines.utils.flood.config import (
    GLOFAS_PRECISION,
    GLOFAS_RESOLUTION,
    GLOFAS_RET_PRD_THRESH_VALS,
)
from data_pipelines.utils.flood.raster_converter import dataset_to_dataframe
from data_pipelines.utils.flood.transforms import add_geometry


@asset(key_prefix=["flood"], compute_kind="xarray", io_manager_key="netcdf_io_manager")
def RP2ythresholds_GloFASv40(context) -> None:
    return None


@asset(key_prefix=["flood"], compute_kind="xarray", io_manager_key="netcdf_io_manager")
def RP5ythresholds_GloFASv40(context) -> None:
    return None


@asset(key_prefix=["flood"], compute_kind="xarray", io_manager_key="netcdf_io_manager")
def RP20ythresholds_GloFASv40(context) -> None:
    return None


@asset(
    ins={
        "RP2ythresholds_GloFASv40": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="xarray",
    io_manager_key="parquet_io_manager",
)
def rp_2y_thresh_pq(context, RP2ythresholds_GloFASv40: xr.Dataset):
    threshold = GLOFAS_RET_PRD_THRESH_VALS[0]
    ds = RP2ythresholds_GloFASv40
    df = dataset_to_dataframe(ds, cols_to_drop=["wgs_1984"], drop_index=False)
    df = df.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        }
    )

    return df


@asset(
    ins={
        "RP5ythresholds_GloFASv40": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="xarray",
    io_manager_key="parquet_io_manager",
)
def rp_5y_thresh_pq(context, RP5ythresholds_GloFASv40: xr.Dataset):
    threshold = GLOFAS_RET_PRD_THRESH_VALS[1]
    ds = RP5ythresholds_GloFASv40
    df = dataset_to_dataframe(ds, cols_to_drop=["wgs_1984"], drop_index=False)
    df = df.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        }
    )

    return df


@asset(
    ins={
        "RP20ythresholds_GloFASv40": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="xarray",
    io_manager_key="parquet_io_manager",
)
def rp_20y_thresh_pq(context, RP20ythresholds_GloFASv40: xr.Dataset):
    threshold = GLOFAS_RET_PRD_THRESH_VALS[2]
    ds = RP20ythresholds_GloFASv40
    df = dataset_to_dataframe(ds, cols_to_drop=["wgs_1984"], drop_index=False)
    df = df.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            f"{threshold}yRP_GloFASv4": f"threshold_{threshold}y",
        }
    )

    return df


@asset(
    ins={
        "rp_2y_thresh_pq": AssetIn(key_prefix="flood"),
        "rp_5y_thresh_pq": AssetIn(key_prefix="flood"),
        "rp_20y_thresh_pq": AssetIn(key_prefix="flood"),
    },
    key_prefix=["flood"],
    compute_kind="dask",
    io_manager_key="parquet_io_manager",
)
def rp_combined_thresh_pq(
    context: AssetExecutionContext,
    dask_resource: DaskResource,
    rp_2y_thresh_pq,
    rp_5y_thresh_pq,
    rp_20y_thresh_pq,
):
    dataframes = [rp_2y_thresh_pq, rp_5y_thresh_pq, rp_20y_thresh_pq]
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
