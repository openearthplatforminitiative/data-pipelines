from dagster import StaticPartitionsDefinition

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

discharge_partitions = StaticPartitionsDefinition(
    [
        "24",
        "48",
        "72",
        "96",
        "120",
        "144",
        "168",
        "192",
        "216",
        "240",
        "264",
        "288",
        "312",
        "336",
        "360",
        "384",
        "408",
        "432",
        "456",
        "480",
        "504",
        "528",
        "552",
        "576",
        "600",
        "624",
        "648",
        "672",
        "696",
        "720",
    ]
)
