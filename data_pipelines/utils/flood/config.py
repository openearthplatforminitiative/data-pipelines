TIMEDELTA = 0
GLOFAS_API_URL = "https://ewds.climate.copernicus.eu/api"
UPSTREAM_URL = "https://confluence.ecmwf.int/download/attachments/242067380/uparea_glofas_v4_0.nc?version=2&modificationDate=1668604690076&api=v2"
GLOFAS_RET_PRD_THRESH_VALS = [2, 5, 20]
GLOFAS_ROI_CENTRAL_AFRICA = {
    "lat_min": -6.0,
    "lat_max": 17.0,
    "lon_min": -18.0,
    "lon_max": 52.0,
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
USE_CONTROL_MEMBER_IN_ENSEMBLE = 1
N_SUBAREAS = 4
