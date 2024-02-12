FROM python:3.11-slim
ENV POETRY_VIRTUALENVS_CREATE=false
WORKDIR /code
RUN pip install poetry
COPY pyproject.toml poetry.lock /code/
RUN poetry install --without dev
COPY data_pipelines/ /code/data_pipelines/