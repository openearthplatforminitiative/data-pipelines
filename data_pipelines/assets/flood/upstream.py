from dagster import asset


@asset(key_prefix=["flood"], compute_kind="xarray", io_manager_key="netcdf_io_manager")
def uparea_glofas_v4_0(context):
    return None
