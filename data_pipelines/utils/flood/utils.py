import xarray as xr


def restrict_dataset_area(
    ds: xr.Dataset,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    buffer: float = 0.0125,
) -> xr.Dataset:
    """
    Restrict the given dataset to the given area.

    - Args:
    - ds (xr.Dataset): The dataset to restrict.
    - lat_min (float): The minimum latitude.
    - lat_max (float): The maximum latitude.
    - lon_min (float): The minimum longitude.
    - lon_max (float): The maximum longitude.
    - buffer (float): The buffer to use for the restriction.

    - Returns:
    xr.Dataset: The restricted dataset.
    """
    return ds.sel(
        latitude=slice(lat_max + buffer, lat_min - buffer),
        longitude=slice(lon_min - buffer, lon_max + buffer),
    )
