from __future__ import annotations

from MaximaDagster.definitions import defs
from MaximaDagster.sensors import (
    calibrant_partitions,
    experiment_partitions,
    xrd_calibration_sensor,
    xrd_experiment_sensor,
)


def test_xrd_job_uses_experiment_dynamic_partitions() -> None:
    resolved_job = defs.resolve_job_def("xrd")
    assert resolved_job.partitions_def is experiment_partitions


def test_calibration_precompute_job_uses_calibrant_partitions() -> None:
    resolved_job = defs.resolve_job_def("calibration_precompute")
    assert resolved_job.partitions_def is calibrant_partitions


def test_defs_register_current_sensor_names() -> None:
    repo_def = defs.get_repository_def()
    names = {sensor_def.name for sensor_def in repo_def.sensor_defs}

    assert xrd_experiment_sensor.name in names
    assert xrd_calibration_sensor.name in names
    assert names == {"xrd_experiment_sensor", "xrd_calibration_sensor"}