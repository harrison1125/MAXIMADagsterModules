import os

from dagster import build_sensor_context

from MaximaDagster.definitions import defs
from MaximaDagster.sensors import calibration_scan_sensor, experiment_folder_sensor, experiment_partitions


class _FakeGirderClient:
    def get(self, route, parameters=None):
        assert route == "folder"
        parent_id = (parameters or {}).get("parentId")

        if parent_id == "root_folder":
            return [
                {"_id": "exp_01", "name": "some_new_experiment_01"},
                {"_id": "calibrants_folder", "name": "calibrants"},
            ]

        if parent_id == "exp_01":
            return [{"_id": "raw_01", "name": "raw"}]

        return []

    def listItem(self, folder_id):
        if folder_id == "calibrants_folder":
            return [{"_id": "cal_item_1", "updated": "2026-03-12T10:00:00.000+00:00"}]
        if folder_id == "raw_01":
            return [{"_id": "item_1"}]
        return []

    def listFile(self, item_id):
        if item_id == "cal_item_1":
            return [{"_id": "cal_file_1", "name": "xrd_calibrant_data_000001.h5", "updated": "2026-03-12T10:00:00.000+00:00"}]
        if item_id == "item_1":
            return [{"_id": "file_1", "name": "scan_point_0_data_00001.h5"}]
        return []


def test_job_uses_experiment_dynamic_partitions():
    resolved_job = defs.resolve_job_def("xrd")
    assert resolved_job.partitions_def is experiment_partitions


def test_calibration_precompute_job_is_registered():
    resolved_job = defs.resolve_job_def("calibration_precompute")
    assert resolved_job.name == "calibration_precompute"


def test_sensor_emits_partitioned_run_request(monkeypatch):
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FakeGirderClient()})
    evaluation = experiment_folder_sensor(context)

    run_requests = evaluation.run_requests
    assert len(run_requests) == 1

    run_request = run_requests[0]
    assert run_request.job_name == "xrd"
    assert run_request.partition_key == "exp_01"
    assert run_request.run_key == "experiment:exp_01"

    assert evaluation.dynamic_partitions_requests
    added = evaluation.dynamic_partitions_requests[0].partition_keys
    assert added == ["exp_01"]

    assert evaluation.cursor
    assert "exp_01" in evaluation.cursor


def test_calibration_sensor_emits_precompute_run_request(monkeypatch):
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FakeGirderClient()})
    evaluation = calibration_scan_sensor(context)

    run_requests = evaluation.run_requests
    assert len(run_requests) == 1

    run_request = run_requests[0]
    assert run_request.job_name == "calibration_precompute"
    assert run_request.run_key == "calibrant:cal_file_1"
    assert run_request.tags["calibrant_scan_file_id"] == "cal_file_1"

    assert evaluation.cursor
    assert "cal_file_1" in evaluation.cursor
