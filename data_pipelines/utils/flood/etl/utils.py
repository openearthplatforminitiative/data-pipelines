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
