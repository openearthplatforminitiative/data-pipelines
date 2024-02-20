from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    base_data_path: str = "s3://openepi-data/"


settings = Settings()
