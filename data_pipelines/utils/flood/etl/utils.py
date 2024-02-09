import xarray as xr


def open_dataset(file_path, engine=None, **kwargs):
    if engine is None:
        engine = determine_engine(file_path)
    return xr.open_dataset(file_path, engine=engine, **kwargs)


def determine_engine(file_path):
    if file_path.endswith(".grib"):
        return "cfgrib"
    elif file_path.endswith(".nc"):
        return "netcdf4"
    else:
        raise ValueError(f"Unrecognized file extension for {file_path}")


def restrict_dataset_area(ds, lat_min, lat_max, lon_min, lon_max, buffer=0.0125):
    return ds.sel(
        latitude=slice(lat_max + buffer, lat_min - buffer),
        longitude=slice(lon_min - buffer, lon_max + buffer),
    )

import pandas as pd
import dask.dataframe as dd

def add_geometry(df: pd.DataFrame | dd.core.DataFrame, half_grid_size: float, precision: int) -> pd.DataFrame | dd.core.DataFrame:
    """
    Add a geometry column to the DataFrame.

    :param df: The DataFrame.
    :param half_grid_size: The half grid size.
    :param precision: The precision to use for rounding.

    :return: The DataFrame with a geometry column.
    """
    df["min_latitude"] = (df["latitude"] - half_grid_size).round(precision)
    df["max_latitude"] = (df["latitude"] + half_grid_size).round(precision)
    df["min_longitude"] = (df["longitude"] - half_grid_size).round(precision)
    df["max_longitude"] = (df["longitude"] + half_grid_size).round(precision)
    
    df["wkt"] = (
        "POLYGON ((" +
        df["min_longitude"].astype(str) + " " +
        df["min_latitude"].astype(str) + "," +
        df["min_longitude"].astype(str) + " " +
        df["max_latitude"].astype(str) + "," +
        df["max_longitude"].astype(str) + " " +
        df["max_latitude"].astype(str) + "," +
        df["max_longitude"].astype(str) + " " +
        df["min_latitude"].astype(str) + "," +
        df["min_longitude"].astype(str) + " " +
        df["min_latitude"].astype(str) +
        "))"
    )
    
    df = df.drop(["min_latitude", "max_latitude", "min_longitude", "max_longitude"], axis=1)
    
    return df