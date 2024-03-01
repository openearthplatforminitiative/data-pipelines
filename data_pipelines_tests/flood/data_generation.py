import numpy as np
import pandas as pd
import xarray as xr


def generate_upstream_filtering_test_data(
    discharge_latitudes,
    discharge_longitudes,
    upstream_latitudes,
    upstream_longitudes,
    num_forecasts=50,
    num_steps=30,
    num_random_cells=5,
    fill_discharge=100.0,
    fill_upstream_threshold=300000.0,
    seed=42,
):
    np.random.seed(seed)

    number = np.arange(num_forecasts)
    step = np.arange(num_steps)

    dis24 = np.full(
        (num_forecasts, num_steps, len(discharge_latitudes), len(discharge_longitudes)),
        fill_discharge,
    )

    ds_discharge = xr.Dataset(
        {"dis24": (("number", "step", "latitude", "longitude"), dis24)},
        coords={
            "number": number,
            "step": step,
            "latitude": discharge_latitudes,
            "longitude": discharge_longitudes,
        },
    )

    uparea = np.full((len(upstream_latitudes), len(upstream_longitudes)), np.nan)

    lat_max_discharge = ds_discharge.latitude.max().item()
    lat_min_discharge = ds_discharge.latitude.min().item()
    lon_max_discharge = ds_discharge.longitude.max().item()
    lon_min_discharge = ds_discharge.longitude.min().item()

    lat_idx_max = np.abs(upstream_latitudes - lat_max_discharge).argmin()
    lat_idx_min = np.abs(upstream_latitudes - lat_min_discharge).argmin()
    lon_idx_max = np.abs(upstream_longitudes - lon_max_discharge).argmin()
    lon_idx_min = np.abs(upstream_longitudes - lon_min_discharge).argmin()

    if lat_idx_min > lat_idx_max:
        lat_idx_min, lat_idx_max = lat_idx_max, lat_idx_min

    if lon_idx_min > lon_idx_max:
        lon_idx_min, lon_idx_max = lon_idx_max, lon_idx_min

    # The area where random cells should fall within
    lat_indices_range = (lat_idx_min, lat_idx_max)
    lon_indices_range = (lon_idx_min, lon_idx_max)

    random_lat_indices = np.random.randint(
        lat_indices_range[0], lat_indices_range[1], num_random_cells
    )
    random_lon_indices = np.random.randint(
        lon_indices_range[0], lon_indices_range[1], num_random_cells
    )

    # Assigning the random cells a value of 300000
    for lat, lon in zip(random_lat_indices, random_lon_indices):
        uparea[lat, lon] = fill_upstream_threshold

    ds_upstream = xr.Dataset(
        {"uparea": (("latitude", "longitude"), uparea)},
        coords={
            "latitude": upstream_latitudes,
            "longitude": upstream_longitudes,
        },
    )

    return ds_discharge, ds_upstream, random_lat_indices, random_lon_indices


def create_ground_truth_upstream_filtering_dataframe(
    ds_discharge,
    random_lat_indices,
    random_lon_indices,
    upstream_latitudes,
    upstream_longitudes,
    fill_discharge=100.0,
):
    number = ds_discharge.number.values
    step = ds_discharge.step.values

    df_data = {"number": [], "step": [], "latitude": [], "longitude": [], "dis24": []}

    # Adding random points to ground truth
    for lat_idx, lon_idx in zip(random_lat_indices, random_lon_indices):
        lat = upstream_latitudes[lat_idx]
        lon = upstream_longitudes[lon_idx]
        for n in number:
            for s in step:
                df_data["number"].append(n)
                df_data["step"].append(s)
                df_data["latitude"].append(lat)
                df_data["longitude"].append(lon)
                df_data["dis24"].append(fill_discharge)

    return pd.DataFrame(df_data)


def generate_restrict_dataset_area_test_dataset(
    lat_min, lat_max, lon_min, lon_max, data_resolution
):
    """
    Generate a test xarray dataset with latitude and longitude points centered
    at increments of 0.025 and 0.075 from the rounded 0.05 values.
    """
    initial_offset = data_resolution / 2
    lat_values = np.arange(
        lat_max + initial_offset,
        lat_min,  # + initial_offset + data_resolution,
        -data_resolution,
    )
    lon_values = np.arange(
        lon_min + initial_offset,
        lon_max,  # + initial_offset + data_resolution,
        data_resolution,
    )

    # Create random data for this range
    data = np.random.rand(len(lat_values), len(lon_values))

    ds = xr.Dataset(
        {"data": (["latitude", "longitude"], data)},
        coords={"latitude": lat_values, "longitude": lon_values},
    )
    return ds
