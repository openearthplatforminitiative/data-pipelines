FROM python:3.12-slim-bookworm

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

# Add newest GDAL and PROJ from unstable repo.
RUN printf "deb http://deb.debian.org/debian/ unstable main contrib non-free" > /etc/apt/sources.list.d/backports.list
RUN apt-get update && apt-get install -y -t unstable \
    gdal-bin \
    libgdal-dev \
    libproj-dev

ENV POETRY_VIRTUALENVS_CREATE=false
WORKDIR /opt/dagster/app
RUN pip install poetry
COPY pyproject.toml poetry.lock /opt/dagster/app/

# Prevent installing binaries for the packages rasterio and fiona
# as they make building on arm macs crash.
RUN poetry config --local installer.no-binary rasterio,fiona,shapely

RUN poetry install --without dev --no-root

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
COPY data_pipelines/ /opt/dagster/app/data_pipelines/
RUN zip -r data_pipelines.zip data_pipelines/

COPY dagster.yaml ${DAGSTER_HOME}
