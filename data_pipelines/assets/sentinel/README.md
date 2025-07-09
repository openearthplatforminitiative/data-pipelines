# Sentinel Pipeline


## Ingest step
The first step ingests sentinel data from copernicus. This is mostly done through the
[sentinel-explorer](https://github.com/openearthplatforminitiative/sentinel2-explorer) repo.
The client requires a copernicus user in order to download sentinel products.

The *raw_ingest* step requires 3 dagster config values:
* area_shp_path: Shapefile of the area that should be covered. The shapefile must contain lat-long coordinates, 
not UTM. This value should be the local path pointing to the file. The path is relative to the root of the data-disk.
* year: The year in which the sentinel images where taken.
* quartile: The quartile in which the sentinel images where taken, i.e Q1, Q2, Q3 or Q4.

Expected runtime for all of brazil: 5-6 hours


## Preprocessing steps

### Extract
Products downloaded from copernicus comes in an archive format. This step extracts the products to a folder.
Will delete the archives afterwards.

Expected runtime for all of brazil: 3-4 hours

### Reproject
This step combines the product bands into one tiff file and reprojects them to mercator, so they can be retiled
later. Also combines all tiff files into one big mosaic in order to avoid retiling problems.

Expected runtime for all of brazil: 10-11 hours

### Retile
The retile step cuts the mosaic into smaller tiles. This step is necessary because the original tiles
do not have enough overlap to cover the entire area after they have been upscaled. This step produces tiles with a
sizeable overlap to prevent future issues. 

This step will produce transparent tiles in areas where the mosaic thinks there is data, but there isnt.
This means this step will require more space than normal.

Expected runtime for all of brazil: 36 hours

### Filter nodata
The filter nodata step deletes the transparent tiles generated in the previous step, as we do not wish to keep them
in further processing steps. This frees up space on the disk and ensures less processing time for next steps.

Expected runtime for all of brazil: 3-4 hours

### Optimize
Converts all tiff files to COG format. This speeds up the upscaling step that will later be performed.
This step is the last preprocessing step, and it will also upload the finished tiles to S3, where they can
be used by different EC2 instances for upscaling

Expected runtime for all of brazil: 36 hours

### Cleanup
This step cleans up the loose files on the disk after the preprocessed data has been uploaded to S3. 
Frees alot of disk space.

Expected runtime for all of brazil: <1 hour

## Upscaling step
The upscaling step super-resolves the sentinel tiles from 10m resolution to either 5m or 2.5m. 
Uses a dask cluster running on EC2 instances. Requires roughly 40 mins of setup before the actual work starts.

Dask cluster information:
* Instance types: g4dn.xlarge
* Filesystem size: 128

The *upscaling* step requires 1 dagster config value:
* model: The model which will be used to upscale the sentinel tiles.

Currently, we support the following models:
* carn_3x3x64g4sw_bootstrap: Non-generative model that produces 5m resolution images
* s2v2x2_spatrad: Generative model that produces 5m resolution
* wsx2_spatrad: Another generative model for 5m images. Produces better images 
in rural areas, but worse images in urban areas.
* wsx4_spatrad: Same as wsx2_spatrad, but generates 2.5m resolution images.

Expected runtime for all of brazil: 43 hours

## Postprocessing steps


### Prepare disk
Downloads all upscaled images to disk in order to have them available for next step.

Expected runtime for all of brazil: 7 hours

### Pyramid
Generates an image pyramid for the upscaled images. This has 3 effects:

1. Compresses the images using JPEG compression. The resulting images will have 90% less
size, with minimal compression artifacts.
2. Splits images into smaller tiles that are easier to load, increasing WMS performance.
3. Computes lower-resolution versions of the images to be loaded when zoomed out.

This process is very slow because of all the downsampling operations that is used.
Changing the downsampling algorithm can make this step faster, but might result in
worse quality downsampled images. These downsampled images are only visible at 
higher zoom levels. Currently, the algorithm used is ```cubic```.
Other algorithms to consider are:

* bilinear: Faster than cubic, but can cause blurring.
* lanczos: Slower than cubic, but creates the best looking images.


Expected runtime for all of brazil using cubic: 222 hours

Expected runtime for all of brazil using bilinear: 120 hours

Expected runtime for all of brazil using lanczos: 370 hours

### Move files and cleanup

Moves finished files from data-processing disk to the data-serving disk, which is
accessible by geoserver. 

Expected runtime for all of brazil: <1 hours

