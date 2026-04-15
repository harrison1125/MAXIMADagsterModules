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


def test_experiment_folder_sensor_bootstrap_sets_cursor_without_run_request(monkeypatch):
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FakeGirderClient()})
    evaluation = experiment_folder_sensor(context)

    run_requests = evaluation.run_requests
    assert run_requests == []

    assert evaluation.dynamic_partitions_requests == []

    assert evaluation.cursor
    assert "exp_01" in evaluation.cursor


def test_calibration_scan_sensor_bootstrap_sets_cursor_without_run_request(monkeypatch):
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FakeGirderClient()})
    evaluation = calibration_scan_sensor(context)

    run_requests = evaluation.run_requests
    assert run_requests == []

    assert evaluation.cursor
    assert "cal_file_1" in evaluation.cursor


def test_experiment_folder_sensor_emits_run_for_new_folder_after_cursor(monkeypatch) -> None:
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    class _SeedClient(_FakeGirderClient):
        pass

    class _NextClient:
        def get(self, route, parameters=None):
            assert route == "folder"
            parent_id = (parameters or {}).get("parentId")

            if parent_id == "root_folder":
                return [
                    {"_id": "exp_01", "name": "some_new_experiment_01"},
                    {"_id": "exp_02", "name": "some_new_experiment_02"},
                    {"_id": "calibrants_folder", "name": "calibrants"},
                ]

            if parent_id in {"exp_01", "exp_02"}:
                return [{"_id": f"raw_{parent_id[-2:]}", "name": "raw"}]

            return []

        def listItem(self, folder_id):
            if folder_id.startswith("raw_"):
                return [{"_id": f"item_{folder_id}"}]
            if folder_id == "calibrants_folder":
                return []
            return []

        def listFile(self, item_id):
            return [{"_id": f"file_{item_id}", "name": "scan_point_0_data_00001.h5"}]

    seed_context = build_sensor_context(resources={"GirderClient": _SeedClient()})
    seed = experiment_folder_sensor(seed_context)

    next_context = build_sensor_context(resources={"GirderClient": _NextClient()}, cursor=seed.cursor)
    evaluation = experiment_folder_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    assert evaluation.run_requests[0].partition_key == "exp_02"
    assert evaluation.dynamic_partitions_requests


def test_calibration_scan_sensor_emits_run_for_new_file_after_cursor(monkeypatch) -> None:
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    class _SeedCalClient:
        def listItem(self, folder_id):
            assert folder_id == "calibrants_folder"
            return [{"_id": "cal_item_1", "updated": "2026-03-12T10:00:00.000+00:00"}]

        def listFile(self, item_id):
            if item_id == "cal_item_1":
                return [{"_id": "cal_file_1", "name": "xrd_calibrant_data_000001.h5", "updated": "2026-03-12T10:00:00.000+00:00"}]
            return []

    class _NextCalClient:
        def listItem(self, folder_id):
            assert folder_id == "calibrants_folder"
            return [
                {"_id": "cal_item_1", "updated": "2026-03-12T10:00:00.000+00:00"},
                {"_id": "cal_item_2", "updated": "2026-03-13T10:00:00.000+00:00"},
            ]

        def listFile(self, item_id):
            if item_id == "cal_item_1":
                return [{"_id": "cal_file_1", "name": "xrd_calibrant_data_000001.h5", "updated": "2026-03-12T10:00:00.000+00:00"}]
            if item_id == "cal_item_2":
                return [{"_id": "cal_file_2", "name": "xrd_calibrant_data_000002.h5", "updated": "2026-03-13T10:00:00.000+00:00"}]
            return []

    seed_context = build_sensor_context(resources={"GirderClient": _SeedCalClient()})
    seed = calibration_scan_sensor(seed_context)

    next_context = build_sensor_context(resources={"GirderClient": _NextCalClient()}, cursor=seed.cursor)
    evaluation = calibration_scan_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    assert evaluation.run_requests[0].run_key == "calibrant:cal_file_2"
