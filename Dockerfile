FROM python:3.11.6-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app
RUN apt-get update && apt-get install -y libeccodes-tools
RUN apt-get update && apt-get install -y zip

ENV POETRY_VIRTUALENVS_CREATE=false
WORKDIR /opt/dagster/app
RUN pip install poetry
COPY pyproject.toml poetry.lock /opt/dagster/app/
RUN poetry install --without dev

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
COPY data_pipelines/ /opt/dagster/app/data_pipelines/
RUN zip -r data_pipelines.zip data_pipelines/

COPY dagster.yaml ${DAGSTER_HOME}
