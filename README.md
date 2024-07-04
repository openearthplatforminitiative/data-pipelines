# data-pipelines

Data pipelines orchestrated by Dagster with Dask functionality for parallelization.

## Running locally

Locally, we can simulate an S3 bucket using [MinIO](https://min.io). Additionally, we can create a local Dask cluster (a combination of a Dask scheduler and one or several workers) either programmatically or using the command line.

A first requirement to running locally is a `.env` file with the following fields:
```
CDS_USER_ID=XXXXXX
CDS_API_KEY=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
base_data_path=s3://my-bucket/my-data-folder
aws_region=foo
aws_access_key_id=youraccesskey
aws_secret_access_key=yoursecretkey
run_local=True
custom_local_dask_cluster=False
custom_local_dask_cluster_address=tcp://127.0.0.1:8787
```
The `CDS_USER_ID` and `CDS_API_KEY` are required to run the flood pipeline which makes requests to the [CDS API](https://cds.climate.copernicus.eu/api-how-to). These two environment variables are read when initalizing the CDS resource upon Dagster startup, so even if the flood pipeline isn't run, they need to be at least defined (e.g., `CDS_USER_ID=foo` and `CDS_API_KEY=bar`). To obtain your own credentials, simply [create your free CDS account](https://cds.climate.copernicus.eu/user/register) and find the `UID` and `API Key` fields at the bottom of your user profile page when logged in.

Next, the fields `base_data_path`, `aws_region`, `aws_access_key_id`, and `aws_secret_access_key` can be set to your liking.

When set to `True`, `run_local` ensures that Dagster communicates with the MinIO storage bucket and creates a local Dask cluster (if necessary, not all assets require a Dask cluster).

When set to `True`, `custom_local_dask_cluster` will require a Dask cluster to be created through the command line at `custom_local_dask_cluster_address`. We will see how to do this. The reason we might want to avoid creating a cluster programatically when running locally is to closer simulate the deployed version of the project, which can be useful for troubleshooting. Indeed, in the production environment, the Dask scheduler and workers are created using `dask scheduler` and `dask worker` commands. On the other hand, when `custom_local_dask_cluster` is set to `False`, a Dask cluster is created programmatically using `dask.distributed.LocalCluster`. This allows for a simpler local setup by avoiding the extra steps through the command line.