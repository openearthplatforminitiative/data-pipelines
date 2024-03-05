from pydantic_settings import BaseSettings
from upath import UPath


class Settings(BaseSettings):
    _base_data_path: str = "s3://openepi-data/"
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    fsspec_cache_storage: str = "/tmp/files"
    dask_cluster_image: str = "astangeland/data-pipelines:latest"

    @property
    def base_data_path(self) -> UPath:
        return UPath(
            self._base_data_path,
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
        )


settings = Settings()
