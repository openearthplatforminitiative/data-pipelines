# data-pipelines

Data pipelines orchestrated by Dagster with Dask functionality for parallelization.

## Running locally

Locally, we can simulate an S3 bucket using [MinIO](https://min.io). Additionally, we can create a local Dask cluster (a combination of a Dask scheduler and one or several workers) either programmatically or using the command line.

A first requirement to running locally is an `.env` file located at the root of this project with the following fields:
```
CDS_USER_ID=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
CDS_API_KEY=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
base_data_path=s3://my-bucket/my-data-folder
aws_region=foo
aws_access_key_id=youraccesskey
aws_secret_access_key=yoursecretkey
run_local=True
custom_local_dask_cluster=False
custom_local_dask_cluster_address=tcp://127.0.0.1:8787
```
The `CDS_USER_ID` and `CDS_API_KEY` are required to run the flood pipeline which makes requests to the [CDS API](https://cds-beta.climate.copernicus.eu/how-to-api). These two environment variables are read when initalizing the CDS resource upon Dagster startup, so even if the flood pipeline isn't run, they need to be at least defined (e.g., `CDS_USER_ID=foo` and `CDS_API_KEY=bar`). To obtain your own credentials, simply [create your free CDS account](https://cds.climate.copernicus.eu/user/register) and find the `UID` and `API Key` fields on your user [profile page](https://cds-beta.climate.copernicus.eu/profile) when logged in.

Next, the fields `base_data_path`, `aws_region`, `aws_access_key_id`, and `aws_secret_access_key` can be set to your liking.

When set to `True`, `run_local` ensures that Dagster communicates with the MinIO storage bucket and creates a local Dask cluster (if necessary, not all assets require a Dask cluster).

When set to `True`, `custom_local_dask_cluster` will require a Dask cluster to be created through the command line at `custom_local_dask_cluster_address`. We will see how to do this. The reason we might want to avoid creating a cluster programatically when running locally is to closer mimic the deployed version of the project, which can be useful for troubleshooting. Indeed, in the production environment, the Dask scheduler and workers are created using `dask scheduler` and `dask worker` commands. On the other hand, when `custom_local_dask_cluster` is set to `False`, a Dask cluster is created programmatically using `dask.distributed.LocalCluster`. This allows for a simpler local setup by avoiding the extra steps through the command line.

### Starting MinIO

First, we can start a docker container running MinIO using the following command:
```
docker run -p 9000:9000 -p 9001:9001 --name minio1 \
  -e "MINIO_ROOT_USER=youraccesskey" \
  -e "MINIO_ROOT_PASSWORD=yoursecretkey" \
  -v /tmp/minio_data:/data \
  minio/minio server /data --console-address ":9001"
```
where `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` should match the `aws_access_key_id` and `aws_secret_access_key` fields, respectively.

We can view and interact with the MinIO storage system at the following address: http://127.0.0.1:9001. The login credentials are the values of `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.

Initially, there is no existing data bucket. This can be created by running the materialization of certain assets in Dagster (we will see an example of one).

### Starting Dagster

In the root of this project, build the Docker image:
```
docker build . -t openepi-dagster
```
Next, run the container:
```
docker run -it --env-file .env -p 3000:3000 -p 8787:8787 openepi-dagster /bin/bash
```
The port `3000` is for the Dagster UI and `8787` is for the Dask cluster. To avoid having to rebuild the image during development, we can also mount the local data_pipelines directory:
```
docker run -it --env-file .env -v $(pwd)/data_pipelines:/opt/dagster/app/data_pipelines -p 3000:3000 -p 8787:8787 openepi-dagster /bin/bash
```
Now we can start Dagster by running:
```
dagster dev --host 0.0.0.0
```
And we open the Dagster UI locally at the following address: http://127.0.0.1:3000.

### Triggering bucket creation

We can materialize an asset to trigger the creation of a data bucket in MinIO by:
- Navigating to the `Assets` tab
- Selecting the `flood / uparea_glofas_v4_0` asset
- Running it using the `Materialize` button

We should see a bucket called `my-bucket` appear in the MinIO interface.

### Dask clusters

Using the above steps to start Dagster would create a Dask cluster programmatically (when necessary). To create the cluster manually, set the `custom_local_dask_cluster` field to `True` in the `.env` file. Then follow the same steps as above. After having started Dagster, identify the `CONTAINER ID` of the `openepi-dagster` Docker container using the `docker ps` command, it should be something like `a1d4c3319da3`.

Then run the following to start an interactive terminal session inside the container:
```
docker exec -it a1d4c3319da3 /bin/bash
```
Then start a custom dask scheduler by running:
```
dask scheduler --host 127.0.0.1 --port 8787 
```
In a separate command line window, start an additional session:
```
docker exec -it a1d4c3319da3 /bin/bash
```
And start a custom dask worker by running:
```
dask worker tcp://127.0.0.1:8787
```
Now, whenever an asset requiring Dask is materialized, it will use the custom cluster running at http://127.0.0.1:8787.

When a Dask asset is materialized, the following line is logged to the Dagster UI:
```
Dask dashboard link: http://127.0.0.1:8787/status
```
The Dask dashboard contains relevant information about the progress and metrics related to Dask-specific computations.

### Precautions to take locally

For both the deforestation and flood pipelines, the file sizes and required compuation can cause the Dask cluster to fail. A simple way to address this is to reduce the amount data used in each pipeline. This can be achieved by commenting out partitions in the the `data_pipelines/partitions.py` file. For deforestation, it is recommended to only keep 1-2 partitions. For flood, 1-6 should be fine.

For the flood pipeline, the amount of data processed can also be reduced by modifying the area of interest in the `data_pipelines/utils/flood/config.py` file through the `GLOFAS_ROI_CENTRAL_AFRICA` field.

Obviously, these approaches should be used to test the execution of the pipeline, not the end result as a lot of data will have been omitted. 

## Flood pipeline

### Job and schedule

The flood pipeline has its own job and schedule defined in the `data_pipelines/jobs.py` file. Here, a job called `all_flood_assets_job` is defined to materialize the `flood/raw_discharge` asset as well as all downstream assets due to the `*` symbol. 

Dagster is aware of downstream assets through explicit dependency declaration. In `data_pipelines/assets/flood/discharge.py`, the `transformed_discharge` asset has two dependencies: `restricted_uparea_glofas_v4_0` and `raw_discharge`. The upstream `restricted_uparea_glofas_v4_0` asset has its own IO manager (defined in `data_pipelines/resources/io_managers.py`). Therefore, it is referenced in the `ins` paremeter to the `@asset` decorator of `transformed_discharge`. Additionally, it is defined as a parameter to the function itself: `restricted_uparea_glofas_v4_0: xr.Dataset`. The result of this is that Dagster automatically identifies IO manager of the upstream asset and uses it to open the upstream asset implicitly before the start of the downstream asset. 

On the other hand, the `raw_discharge` asset is made up of several GRIB files and an the reading of these files has to be made manually. Therefore, `raw_discharge` is only referenced in the `deps` parameter to the `@asset` decorator. 

By defining assets in this way, Dagster can create asset lineages/graphs.

The flood job also has a timeout of 3600 seconds (1h) to avoid running indefinitely. The job itself is triggered by a Dagster schedule running at 09:30 UTC every day.

### Flood running locally

When running the flood pipeline locally, the MinIO bucket should be created using the [bucket creation step](#triggering-bucket-creation). After this, the files `RP20ythresholds_GloFASv40.nc`, `RP5ythresholds_GloFASv40.nc`, and `RP2ythresholds_GloFASv40.nc` need to be manually uploaded (e.g., drag and dropped in the MinIO interface) to the folder `my-bucket/my-data-folder/flood`. This is because these files currently cannot be downloaded from an open API. These files can be found in the `auxiliary_data/flood` folder at the root of this project.