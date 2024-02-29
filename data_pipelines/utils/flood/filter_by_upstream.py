from data_pipelines.utils.flood.utils import restrict_dataset_area
import xarray as xr


def get_filtered_discharge_from_files(
    discharge_file_path: str,
    upstream_file_path: str,
    threshold_area: int = 250 * 1e6,
    discharge_engine: str = "cfgrib",
    upstream_engine: str = "netcdf4",
) -> xr.Dataset:
    """
    Get the filtered discharge data from the given files.
    The discharge data is filtered by the upstream area dataset
    using the given upstream threshold area.

    - Args:
    - discharge_file_path (str): The path to the discharge file.
    - upstream_file_path (str): The path to the upstream file.
    - threshold_area (float): The threshold area for the upstream filtering.
    - discharge_engine (str): The engine to use for reading the discharge file.
    - upstream_engine (str): The engine to use for reading the upstream file.

    - Returns:
    xr.Dataset: The filtered discharge data.
    """
    ds_discharge = xr.open_dataset(discharge_file_path, engine=discharge_engine)
    ds_upstream = xr.open_dataset(upstream_file_path, engine=upstream_engine)

    return apply_upstream_threshold(
        ds_discharge, ds_upstream, threshold_area=threshold_area
    )


def apply_upstream_threshold(
    ds_discharge: xr.Dataset,
    ds_upstream: xr.Dataset,
    threshold_area: int = 250 * 1e6,
    buffer: float = 0.0125,
) -> xr.Dataset:
    """
    Apply the upstream threshold to the given discharge dataset.

    - Args:
    - ds_discharge (xr.Dataset): The discharge dataset.
    - ds_upstream (xr.Dataset): The upstream area dataset.
    - threshold_area (float): The threshold area for the upstream filtering.
    - buffer (float): The buffer to use for the upstream filtering.

    - Returns:
    xr.Dataset: The filtered discharge dataset.
    """
    subset_uparea = restrict_dataset_area(
        ds_upstream["uparea"],
        ds_discharge.latitude.min(),
        ds_discharge.latitude.max(),
        ds_discharge.longitude.min(),
        ds_discharge.longitude.max(),
        buffer,
    )

    subset_uparea_aligned = subset_uparea.reindex(
        latitude=ds_discharge["dis24"].latitude,
        longitude=ds_discharge["dis24"].longitude,
        method="nearest",
    )

    mask = subset_uparea_aligned >= threshold_area

    ds_discharge["dis24"] = ds_discharge["dis24"].where(mask)

    return ds_discharge
