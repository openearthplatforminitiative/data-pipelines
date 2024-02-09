from data_pipelines.utils.flood.etl.utils import open_dataset, restrict_dataset_area


def get_filtered_discharge_from_files(
    discharge_file_path,
    upstream_file_path,
    threshold_area=250 * 1e6,
    discharge_engine="cfgrib",
    upstream_engine="netcdf4",
):
    ds_discharge = open_dataset(discharge_file_path, engine=discharge_engine)
    ds_upstream = open_dataset(upstream_file_path, engine=upstream_engine)

    return apply_upstream_threshold(
        ds_discharge, ds_upstream, threshold_area=threshold_area
    )


def apply_upstream_threshold(
    ds_discharge, ds_upstream, threshold_area=250 * 1e6, buffer=0.0125
):
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