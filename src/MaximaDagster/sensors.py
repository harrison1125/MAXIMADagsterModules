import json
from datetime import datetime, timedelta, timezone
from typing import Any

from dagster import (
    DynamicPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from .utils.discovery import (
    call_with_retries,
    fetch_partitions
)


def _parse_cursor_payload(cursor: str | None) -> dict[str, Any]:
    if not cursor:
        return {}
    try:
        payload = json.loads(cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _default_since() -> str:
    return "1970-01-01T00:00:00.000000+00:00"


def _next_since() -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()


def _parse_checksum_map(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    checksums: dict[str, str] = {}
    for key, checksum in value.items():
        partition_key = str(key or "").strip()
        checksum_text = str(checksum or "").strip()
        if not partition_key or not checksum_text:
            continue
        checksums[partition_key] = checksum_text
    return checksums


def _parse_girder_cursor(cursor: str | None) -> tuple[str, dict[str, str]]:
    payload = _parse_cursor_payload(cursor)
    since = str(payload.get("since") or "")
    checksums_by_partition = _parse_checksum_map(payload.get("checksums_by_partition"))
    return since, checksums_by_partition


def _serialize_girder_cursor(since: str, checksums_by_partition: dict[str, str]) -> str:
    return json.dumps(
        {
            "since": since,
            "checksums_by_partition": dict(sorted(checksums_by_partition.items())),
        }
    )


def build_girder_partition_sensor(
    sensor_name: str,
    job_name: str,
    data_type: str,
    partitions_def: DynamicPartitionsDefinition,
):
    """
    Factory function to generate a dynamically partitioned Girder sensor.
    """
    @sensor(
        name=sensor_name,
        job_name=job_name,
        minimum_interval_seconds=30,
        required_resource_keys={"GirderClient"},
    )
    def _generic_sensor(context: SensorEvaluationContext, GirderClient=None):
        gc = GirderClient or context.resources.GirderClient

        since, checksums_by_partition = _parse_girder_cursor(context.cursor)
        poll_since = since or _default_since()

        partition_updates = call_with_retries(
            fetch_partitions,
            gc,
            data_type=data_type,
            since=poll_since,
        )

        merged_checksums = dict(checksums_by_partition)
        merged_checksums.update(partition_updates)

        changed_partition_keys = [
            partition_key
            for partition_key, checksum in partition_updates.items()
            if checksums_by_partition.get(partition_key) != checksum
        ]

        if not context.cursor or not changed_partition_keys:
            return SensorResult(
                cursor=_serialize_girder_cursor(_next_since(), merged_checksums),
                run_requests=[],
                dynamic_partitions_requests=[],
            )

        existing_partitions = set(context.instance.get_dynamic_partitions(partitions_def.name))
        new_partitions = [
            partition_key 
            for partition_key in changed_partition_keys 
            if partition_key not in existing_partitions
        ]

        run_requests = [
            RunRequest(
                run_key=f"{data_type}:{partition_key}:{partition_updates[partition_key]}",
                job_name=job_name,
                partition_key=partition_key,
                tags={
                    "partition_key": partition_key,
                    "data_checksum": partition_updates[partition_key],
                    "data_type": data_type,
                },
            )
            for partition_key in changed_partition_keys
        ]

        dynamic_requests = []
        if new_partitions:
            dynamic_requests = [partitions_def.build_add_request(new_partitions)]

        return SensorResult(
            cursor=_serialize_girder_cursor(_next_since(), merged_checksums),
            run_requests=run_requests,
            dynamic_partitions_requests=dynamic_requests,
        )

    return _generic_sensor


experiment_partitions = DynamicPartitionsDefinition(name="experiments")
calibrant_partitions = DynamicPartitionsDefinition(name="calibrants")


xrd_experiment_sensor = build_girder_partition_sensor(
    sensor_name="xrd_experiment_sensor",
    job_name="xrd",
    data_type="xrd_raw",
    partitions_def=experiment_partitions,
)

xrd_calibration_sensor = build_girder_partition_sensor(
    sensor_name="xrd_calibration_sensor",
    job_name="calibration_precompute",
    data_type="xrd_calibrant_raw",
    partitions_def=calibrant_partitions,
)