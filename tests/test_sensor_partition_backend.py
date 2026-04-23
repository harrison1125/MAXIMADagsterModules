from __future__ import annotations

import json

from dagster import DagsterInstance, build_sensor_context

from MaximaDagster.sensors import xrd_calibration_sensor, xrd_experiment_sensor


class _PartitionClient:
    def __init__(self, responses_by_data_type):
        self.responses_by_data_type = responses_by_data_type

    def get(self, route, parameters=None):
        assert route == "aimdl/partition"
        data_type = (parameters or {}).get("dataType")
        return dict(self.responses_by_data_type.get(data_type, {}))


def test_xrd_sensor_bootstrap_sets_cursor_without_runs() -> None:
    client = _PartitionClient({"xrd_raw": {"exp_01": "chk_1"}})

    context = build_sensor_context(resources={"GirderClient": client})
    evaluation = xrd_experiment_sensor(context)

    assert evaluation.run_requests == []
    assert evaluation.dynamic_partitions_requests == []
    assert evaluation.cursor
    payload = json.loads(evaluation.cursor)
    assert set(payload.keys()) == {"since", "checksums_by_partition"}
    assert payload["checksums_by_partition"] == {"exp_01": "chk_1"}


def test_xrd_sensor_checksum_change_triggers_run() -> None:
    with DagsterInstance.ephemeral() as instance:
        seed_client = _PartitionClient({"xrd_raw": {"exp_01": "chk_1"}})
        seed_context = build_sensor_context(resources={"GirderClient": seed_client}, instance=instance)
        seed = xrd_experiment_sensor(seed_context)

        next_client = _PartitionClient({"xrd_raw": {"exp_01": "chk_2"}})
        next_context = build_sensor_context(
            resources={"GirderClient": next_client},
            cursor=seed.cursor,
            instance=instance,
        )
        evaluation = xrd_experiment_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    request = evaluation.run_requests[0]
    assert request.partition_key == "exp_01"
    assert request.run_key == "xrd_raw:exp_01:chk_2"
    assert request.tags == {
        "partition_key": "exp_01",
        "data_checksum": "chk_2",
        "data_type": "xrd_raw",
    }
    assert evaluation.dynamic_partitions_requests
    assert evaluation.dynamic_partitions_requests[0].partition_keys == ["exp_01"]


def test_xrd_sensor_unchanged_checksum_emits_no_runs() -> None:
    with DagsterInstance.ephemeral() as instance:
        seed_client = _PartitionClient({"xrd_raw": {"exp_01": "chk_1"}})
        seed_context = build_sensor_context(resources={"GirderClient": seed_client}, instance=instance)
        seed = xrd_experiment_sensor(seed_context)

        next_client = _PartitionClient({"xrd_raw": {"exp_01": "chk_1"}})
        next_context = build_sensor_context(
            resources={"GirderClient": next_client},
            cursor=seed.cursor,
            instance=instance,
        )
        evaluation = xrd_experiment_sensor(next_context)

    assert evaluation.run_requests == []
    assert evaluation.dynamic_partitions_requests == []


def test_xrd_sensor_new_partition_adds_only_new_partition() -> None:
    with DagsterInstance.ephemeral() as instance:
        seed_client = _PartitionClient({"xrd_raw": {"exp_01": "chk_1"}})
        seed_context = build_sensor_context(resources={"GirderClient": seed_client}, instance=instance)
        seed = xrd_experiment_sensor(seed_context)

        next_client = _PartitionClient({"xrd_raw": {"exp_01": "chk_1", "exp_02": "chk_9"}})
        next_context = build_sensor_context(
            resources={"GirderClient": next_client},
            cursor=seed.cursor,
            instance=instance,
        )
        evaluation = xrd_experiment_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    assert evaluation.run_requests[0].partition_key == "exp_02"
    assert evaluation.dynamic_partitions_requests
    assert evaluation.dynamic_partitions_requests[0].partition_keys == ["exp_02"]


def test_calibration_sensor_bootstrap_sets_cursor_without_runs() -> None:
    client = _PartitionClient({"xrd_calibrant_raw": {"cal_01": "chk_a"}})

    context = build_sensor_context(resources={"GirderClient": client})
    evaluation = xrd_calibration_sensor(context)

    assert evaluation.run_requests == []
    assert evaluation.dynamic_partitions_requests == []
    payload = json.loads(evaluation.cursor)
    assert payload["checksums_by_partition"] == {"cal_01": "chk_a"}


def test_calibration_sensor_checksum_change_triggers_run() -> None:
    with DagsterInstance.ephemeral() as instance:
        seed_client = _PartitionClient({"xrd_calibrant_raw": {"cal_01": "chk_a"}})
        seed_context = build_sensor_context(resources={"GirderClient": seed_client}, instance=instance)
        seed = xrd_calibration_sensor(seed_context)

        next_client = _PartitionClient({"xrd_calibrant_raw": {"cal_01": "chk_b"}})
        next_context = build_sensor_context(
            resources={"GirderClient": next_client},
            cursor=seed.cursor,
            instance=instance,
        )
        evaluation = xrd_calibration_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    request = evaluation.run_requests[0]
    assert request.partition_key == "cal_01"
    assert request.run_key == "xrd_calibrant_raw:cal_01:chk_b"
    assert request.tags == {
        "partition_key": "cal_01",
        "data_checksum": "chk_b",
        "data_type": "xrd_calibrant_raw",
    }