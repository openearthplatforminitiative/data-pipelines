FROM python:3.11.6-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

ENV POETRY_VIRTUALENVS_CREATE=false
WORKDIR /opt/dagster/app
RUN pip install poetry
COPY pyproject.toml poetry.lock /opt/dagster/app/
RUN poetry install --without dev

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
COPY data_pipelines/ /opt/dagster/app/data_pipelines/
COPY dagster.yaml ${DAGSTER_HOME}
