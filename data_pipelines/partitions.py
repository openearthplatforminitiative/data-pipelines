from dagster import StaticPartitionsDefinition
from data_pipelines.utils.flood.config import LEADTIME_HOURS

gfc_area_partitions = StaticPartitionsDefinition(
    [
        "10N_020W",
        "10N_010W",
        "10N_000E",
        "10N_010E",
        "10N_020E",
        "10N_030E",
        "10N_040E",
        "00N_000E",
        "00N_010E",
        "00N_020E",
        "00N_030E",
        "00N_040E",
    ]
)

discharge_partitions = StaticPartitionsDefinition(LEADTIME_HOURS)
