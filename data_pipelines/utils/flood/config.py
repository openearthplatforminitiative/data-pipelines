# PYTHON_PREFIX = "/dbfs"
PYTHON_PREFIX = "/home/giltinde"  # "/Users/gil"
# DBUTILS_PREFIX = "dbfs:"
DBUTILS_PREFIX = "/home/giltinde"  # "/Users/gil"
GLOFAS_API_URL = "https://cds.climate.copernicus.eu/api/v2"
S3_OPEN_EPI_PATH = "mnt/openepi-storage"
S3_GLOFAS_PATH = "mnt/openepi-storage/glofas"
S3_GLOFAS_DOWNLOADS_PATH = "tmp/openepi-storage/glofas/api-downloads"
S3_GLOFAS_AUX_DATA_PATH = "mnt/openepi-storage/glofas/auxiliary-data"
S3_GLOFAS_FILTERED_PATH = "tmp/openepi-storage/glofas/filtered"
S3_GLOFAS_PROCESSED_PATH = "tmp/openepi-storage/glofas/processed"
GLOFAS_PROCESSED_SUMMARY_FORECAST_FILENAME = "processed_summary_forecast.parquet"
GLOFAS_PROCESSED_DETAILED_FORECAST_FILENAME = "processed_detailed_forecast.parquet"
GLOFAS_UPSTREAM_FILENAME = "uparea_glofas_v4_0.nc"
GLOFAS_RET_PRD_THRESH_VALS = [2, 5, 20]
GLOFAS_RET_PRD_THRESH_RASTER_FILENAMES = {
    "2": "RP2ythresholds_GloFASv40.nc",
    "5": "RP5ythresholds_GloFASv40.nc",
    "20": "RP20ythresholds_GloFASv40.nc",
}
GLOFAS_RET_PRD_THRESH_PARQUET_FILENAMES = {
    "2": "RP2ythresholds_GloFASv40.parquet",
    "5": "RP5ythresholds_GloFASv40.parquet",
    "20": "RP20ythresholds_GloFASv40.parquet",
}
GLOFAS_PROCESSED_THRESH_FILENAME = "processed_thresholds.parquet"
# GLOFAS_ROI_CENTRAL_AFRICA = {
#     "lat_min": -6.0,
#     "lat_max": 17.0,
#     "lon_min": -18.0,
#     "lon_max": 52.0,
# }

GLOFAS_ROI_CENTRAL_AFRICA = {
    "lat_min": -3.8,
    "lat_max": -2.0,
    "lon_min": 33.5,
    "lon_max": 34.5,
}

GLOFAS_RESOLUTION = 0.05
GLOFAS_PRECISION = 3
GLOFAS_BUFFER_DIV = 4
GLOFAS_BUFFER_MULT = 2
GLOFAS_UPSTREAM_THRESHOLD = 250000000
GLOFAS_FLOOD_TENDENCIES = {"increasing": "U", "stagnant": "C", "decreasing": "D"}
GLOFAS_FLOOD_INTENSITIES = {"purple": "P", "red": "R", "yellow": "Y", "gray": "G"}
GLOFAS_FLOOD_PEAK_TIMINGS = {
    "black_border": "BB",
    "grayed_color": "GC",
    "gray_border": "GB",
}
USE_FIRST_AS_CONTROL = 1
USE_CONTROL_MEMBER_IN_ENSEMBLE = 1
LEADTIME_HOURS = [
    "24",
    "48",
    "72",
    "96",
    "120",
    "144",
    "168",
    "192",
    "216",
    "240",
    "264",
    "288",
    "312",
    "336",
    "360",
    "384",
    "408",
    "432",
    "456",
    "480",
    "504",
    "528",
    "552",
    "576",
    "600",
    "624",
    "648",
    "672",
    "696",
    "720",
]
# LEADTIME_HOURS = [
# "24",
# "48",
#   "72",
#   "96",
#   "120",
#    "144",
#    "168",
# ]
