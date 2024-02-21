from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    base_data_path: str = "s3://openepi-data/"
    dask_cluster_image: str = "astangeland/data-pipelines:latest"


settings = Settings()
