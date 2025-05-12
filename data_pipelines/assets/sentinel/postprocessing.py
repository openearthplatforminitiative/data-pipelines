from dagster import asset, AssetExecutionContext, AssetIn
from data_pipelines.assets.sentinel.config import CutlineConfig, PyramidConfig
import os

datapath = os.getenv('SENTINEL_DATA_DIR')
servingpath = os.getenv('SENTINEL_SERVING_DIR')


#@asset(
#    ins={
#        "upscale": AssetIn(key_prefix="sentinel"),
#    },
#    key_prefix=["sentinel"]
#)
#def postprocess_cutline(context: AssetExecutionContext, config: CutlineConfig, upscale: list):
#    os.system("gdalwarp -overwrite -multi -of GTiff -cutline %s -crop_to_cutline %s" %
#              (config.crop_shp_file, ' '.join(upscale)))


@asset(
    ins={
        "upscale": AssetIn(key_prefix="sentinel"),
    },
    key_prefix=["sentinel"]
)
def postprocess_pyramid(context: AssetExecutionContext, config: PyramidConfig, upscale: list):
    vrt_path = f"{datapath}/finished.vrt"
    pyramid_dir = f"{servingpath}/{config.pyramid_folder}"

    os.system("gdalbuildvrt %s %s" % (vrt_path, ' '.join(upscale)))

    os.makedirs(pyramid_dir, exist_ok=True)

    os.system("gdal_retile.py -v -r bilinear -levels 6 -ps 2048 2048 -co 'TILED=YES' -co 'COMPRESS=JPEG' -targetDir %s %s" %
              (pyramid_dir, vrt_path))
