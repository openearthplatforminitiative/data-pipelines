FROM python:3.11.6-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app
# Some poetry packages require the following installations
RUN apt-get update && apt-get install -y \
    libeccodes-tools \
    zip \
    gcc \
    g++ \
    binutils \
    libproj-dev \
    gdal-bin \
    libgdal-dev

ENV POETRY_VIRTUALENVS_CREATE=false
WORKDIR /opt/dagster/app
RUN pip install poetry
COPY pyproject.toml poetry.lock /opt/dagster/app/

# Prevent installing binaries for the packages rasterio and fiona
# as they make building on arm macs crash.
RUN poetry config --local installer.no-binary rasterio,fiona

RUN poetry install --without dev

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
COPY data_pipelines/ /opt/dagster/app/data_pipelines/
RUN zip -r data_pipelines.zip data_pipelines/

COPY dagster.yaml ${DAGSTER_HOME}
