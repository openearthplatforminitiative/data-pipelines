from dagster import Config


class IngestConfig(Config):
    year: int
    quartile: str
    area_shp_path: str


class CutlineConfig(Config):
    crop_shp_file: str


class PyramidConfig(Config):
    pyramid_folder: str


class UpscaleConfig(Config):
    model: str
