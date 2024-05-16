from datetime import datetime

from dagster import (
    AssetKey,
    DagsterEventType,
    DagsterInstance,
    EventRecordsFilter,
    RunRequest,
    SensorEvaluationContext,
    build_schedule_from_partitioned_job,
    define_asset_job,
    sensor,
)

from data_pipelines.partitions import discharge_partitions

# Define the job for raw_discharge with the concurrency limit
raw_discharge_job = define_asset_job(
    "raw_discharge_job",
    selection=["flood/raw_discharge", "flood/transformed_discharge"],
    partitions_def=discharge_partitions,
    tags={"sequential_backfill": "true"},
)

# Define the schedule for raw_discharge
raw_discharge_daily_schedule = build_schedule_from_partitioned_job(
    job=raw_discharge_job,
    cron_schedule="09 11 * * *",
    execution_timezone="UTC",
)

# Define the job for downstream assets
downstream_assets_job = define_asset_job(
    "downstream_assets_job",
    selection="flood/split_discharge_by_area*",
)


def _are_all_partitions_materialized(
    upstream_asset_key: AssetKey,
    partition_keys: list,
    instance: DagsterInstance,
    current_day: datetime.date,
    context,
) -> bool:
    """Check if all partitions of an asset have been materialized today.

    Args:
        upstream_asset_key (AssetKey): The asset key of the upstream asset.
        partition_keys (list): The list of partition keys to check.
        instance (DagsterInstance): The current Dagster instance.
        current_day (datetime.date): The current day.
        context: The sensor execution context.

    Returns:
        bool: True if all partitions have been materialized today, False otherwise.
    """
    all_partitions_materialized_today = True
    for partition_key in partition_keys:
        events = instance.get_event_records(
            EventRecordsFilter(
                asset_key=upstream_asset_key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_partitions=[partition_key],
            ),
            limit=1,
        )
        context.log.info(f"Partition {partition_key} events: {events}")
        if not events:
            context.log.info(f"Partition {partition_key} has no events.")
            all_partitions_materialized_today = False
            break
        # Convert the Unix timestamp to a datetime object
        event_timestamp = datetime.fromtimestamp(events[0].timestamp)
        context.log.info(
            f"Partition {partition_key} event timestamp: {event_timestamp}"
        )
        if event_timestamp.date() != current_day:
            context.log.info(
                f"Partition {partition_key} event timestamp does not match current day."
            )
            all_partitions_materialized_today = False
            break
    context.log.info(
        f"All partitions materialized today: {all_partitions_materialized_today}"
    )
    return all_partitions_materialized_today


@sensor(job=downstream_assets_job)
def downstream_asset_sensor(context: SensorEvaluationContext):
    """
    Sensor to check if all partitions of the upstream asset have been materialized today.
    If all partitions have been materialized, yield a RunRequest to materialize the downstream asset.

    Args:
        context (SensorExecutionContext): The sensor execution context.

    Yields:
        RunRequest: The RunRequest to materialize the downstream asset.
    """
    context.log.info(f"Context Type: {type(context)}")
    instance = context.instance
    context.log.info(f"Sensor execution context: {context}")
    context.log.info(f"Dagster Instance str: {instance.info_str()}")
    all_events = instance.get_event_records(
        EventRecordsFilter(DagsterEventType.ASSET_MATERIALIZATION)
    )
    context.log.info(f"All events: {all_events}")
    upstream_asset_key = AssetKey(["flood", "transformed_discharge"])
    partition_keys = discharge_partitions.get_partition_keys()
    current_day = datetime.now().date()  # Get the current day

    context.log.info(f"Current day: {current_day}")
    context.log.info(f"Partition keys: {partition_keys}")

    # Check if all partitions of the upstream asset have been materialized today
    all_partitions_materialized_today = _are_all_partitions_materialized(
        upstream_asset_key, partition_keys, instance, current_day, context
    )

    context.log.info(
        f"All partitions materialized today: {all_partitions_materialized_today}"
    )

    if all_partitions_materialized_today:
        current_day_str = current_day.isoformat()
        new_cursor = f"materialized_up_to_{current_day_str}"
        last_materialization_cursor = context.cursor

        context.log.info(f"Last materialization cursor: {last_materialization_cursor}")
        context.log.info(f"New cursor: {new_cursor}")

        # Only yield a RunRequest if the cursor has not been updated to the new value
        if last_materialization_cursor != new_cursor:
            context.log.info(
                "Yielding a RunRequest to materialize the downstream asset."
            )
            run_key = f"downstream_asset_materialization_{current_day_str}"
            yield RunRequest(
                run_key=run_key,  # Use the current day as the run_key
                run_config={
                    # Include any necessary run configuration here
                },
            )
            # Update the cursor to the new value
            context.update_cursor(new_cursor)
