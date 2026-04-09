from dagster import SkipReason, build_sensor_context

from MaximaDagster.sensors import calibration_scan_sensor, experiment_folder_sensor


class _DatafilesOnlyClient:
    def __init__(self, rows_by_type=None, files_by_item=None):
        self.rows_by_type = rows_by_type or {}
        self.files_by_item = files_by_item or {}
        self.calls = []

    def get(self, route, parameters=None):
        self.calls.append((route, parameters or {}))
        if route != "aimdl/datafiles":
            raise AssertionError(f"Unexpected route {route}")

        data_type = (parameters or {}).get("dataType")
        offset = (parameters or {}).get("offset", 0)
        if offset != 0:
            return []

        return list(self.rows_by_type.get(data_type, []))

    def getFolder(self, folder_id):
        if folder_id == "raw_01":
            return {"_id": "raw_01", "parentId": "exp_01", "name": "raw"}
        if folder_id == "exp_01":
            return {"_id": "exp_01", "name": "exp one"}
        raise Exception(f"Unknown folder {folder_id}")

    def listFile(self, item_id):
        return list(self.files_by_item.get(item_id, []))


class _FailingDatafilesWithLegacyClient:
    def get(self, route, parameters=None):
        if route == "aimdl/datafiles":
            raise RuntimeError("endpoint unavailable")

        if route == "folder":
            parent_id = (parameters or {}).get("parentId")
            if parent_id == "root_folder":
                return [{"_id": "exp_legacy", "name": "legacy exp"}]
            if parent_id == "exp_legacy":
                return [{"_id": "raw_legacy", "name": "raw"}]
            return []

        return []

    def getFolder(self, folder_id):
        if folder_id == "raw_legacy":
            return {"_id": "raw_legacy", "parentId": "exp_legacy", "name": "raw"}
        if folder_id == "exp_legacy":
            return {"_id": "exp_legacy", "name": "legacy exp"}
        raise Exception(f"Unknown folder {folder_id}")

    def listItem(self, folder_id):
        if folder_id == "raw_legacy":
            return [{"_id": "legacy_item_1"}]
        if folder_id == "calibrants_folder":
            return [{"_id": "legacy_cal_item_1", "updated": "2026-03-12T10:00:00.000+00:00"}]
        return []

    def listFile(self, item_id):
        if item_id == "legacy_item_1":
            return [{"_id": "legacy_file_1", "name": "scan_point_0_data_00001.h5"}]
        if item_id == "legacy_cal_item_1":
            return [
                {
                    "_id": "legacy_cal_file_1",
                    "name": "xrd_calibrant_data_000001.h5",
                    "updated": "2026-03-12T10:00:00.000+00:00",
                }
            ]
        return []


def test_experiment_sensor_uses_datafiles_backend(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_raw": [
                {
                    "_id": "file_001",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                    "folderId": "raw_01",
                    "experimentFolderId": "exp_01",
                    "experimentFolderName": "exp one",
                }
            ]
        }
    )
    context = build_sensor_context(resources={"GirderClient": client})
    evaluation = experiment_folder_sensor(context)

    assert evaluation.run_requests == []
    assert evaluation.dynamic_partitions_requests == []
    assert evaluation.cursor
    assert "exp_01" in evaluation.cursor


def test_experiment_sensor_emits_run_for_newer_datafile_after_cursor(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    seed_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_raw": [
                {
                    "_id": "file_001",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                    "folderId": "raw_01",
                    "experimentFolderId": "exp_01",
                    "experimentFolderName": "exp one",
                }
            ]
        }
    )
    seed_context = build_sensor_context(resources={"GirderClient": seed_client})
    seed_evaluation = experiment_folder_sensor(seed_context)

    next_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_raw": [
                {
                    "_id": "file_001",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                    "folderId": "raw_01",
                    "experimentFolderId": "exp_01",
                    "experimentFolderName": "exp one",
                },
                {
                    "_id": "file_002",
                    "name": "scan_point_0_data_00002.h5",
                    "created": "2026-03-13T10:00:00.000+00:00",
                    "folderId": "raw_02",
                    "experimentFolderId": "exp_02",
                    "experimentFolderName": "exp two",
                },
            ]
        }
    )
    next_context = build_sensor_context(resources={"GirderClient": next_client}, cursor=seed_evaluation.cursor)
    evaluation = experiment_folder_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    run_request = evaluation.run_requests[0]
    assert run_request.run_key == "experiment:exp_02"
    assert run_request.partition_key == "exp_02"
    assert evaluation.dynamic_partitions_requests
    assert evaluation.dynamic_partitions_requests[0].partition_keys == ["exp_02"]


def test_experiment_sensor_ignores_late_older_datafile_after_cursor(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    seed_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_raw": [
                {
                    "_id": "file_001",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                    "folderId": "raw_01",
                    "experimentFolderId": "exp_01",
                    "experimentFolderName": "exp one",
                }
            ]
        }
    )
    seed_context = build_sensor_context(resources={"GirderClient": seed_client})
    seed_evaluation = experiment_folder_sensor(seed_context)

    late_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_raw": [
                {
                    "_id": "file_002",
                    "name": "scan_point_0_data_00002.h5",
                    "created": "2026-03-11T10:00:00.000+00:00",
                    "folderId": "raw_02",
                    "experimentFolderId": "exp_02",
                    "experimentFolderName": "exp two",
                }
            ]
        }
    )
    late_context = build_sensor_context(resources={"GirderClient": late_client}, cursor=seed_evaluation.cursor)
    evaluation = experiment_folder_sensor(late_context)

    assert isinstance(evaluation, SkipReason)


def test_calibration_sensor_uses_datafiles_backend(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_calibrant_raw": [
                {
                    "_id": "cal_file_001",
                    "name": "xrd_calibrant_data_000001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                }
            ]
        },
        files_by_item={
            "cal_file_001": [{"_id": "cal_file_001_data", "name": "xrd_calibrant_data_000001.h5"}]
        },
    )
    context = build_sensor_context(resources={"GirderClient": client})
    evaluation = calibration_scan_sensor(context)

    assert evaluation.run_requests == []
    assert evaluation.cursor
    assert "cal_file_001_data" in evaluation.cursor


def test_calibration_sensor_emits_run_for_newer_datafile_after_cursor(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    seed_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_calibrant_raw": [
                {
                    "_id": "cal_file_001",
                    "name": "xrd_calibrant_data_000001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                }
            ]
        },
        files_by_item={
            "cal_file_001": [{"_id": "cal_file_001_data", "name": "xrd_calibrant_data_000001.h5"}]
        },
    )
    seed_context = build_sensor_context(resources={"GirderClient": seed_client})
    seed_evaluation = calibration_scan_sensor(seed_context)

    next_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_calibrant_raw": [
                {
                    "_id": "cal_file_001",
                    "name": "xrd_calibrant_data_000001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                },
                {
                    "_id": "cal_file_002",
                    "name": "xrd_calibrant_data_000002.h5",
                    "created": "2026-03-13T10:00:00.000+00:00",
                },
            ]
        },
        files_by_item={
            "cal_file_001": [{"_id": "cal_file_001_data", "name": "xrd_calibrant_data_000001.h5"}],
            "cal_file_002": [{"_id": "cal_file_002_data", "name": "xrd_calibrant_data_000002.h5"}],
        },
    )
    next_context = build_sensor_context(resources={"GirderClient": next_client}, cursor=seed_evaluation.cursor)
    evaluation = calibration_scan_sensor(next_context)

    assert len(evaluation.run_requests) == 1
    run_request = evaluation.run_requests[0]
    assert run_request.run_key == "calibrant:cal_file_002_data"
    assert run_request.tags["calibrant_scan_file_id"] == "cal_file_002_data"


def test_calibration_sensor_ignores_late_older_datafile_after_cursor(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    seed_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_calibrant_raw": [
                {
                    "_id": "cal_file_001",
                    "name": "xrd_calibrant_data_000001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                }
            ]
        },
        files_by_item={
            "cal_file_001": [{"_id": "cal_file_001_data", "name": "xrd_calibrant_data_000001.h5"}]
        },
    )
    seed_context = build_sensor_context(resources={"GirderClient": seed_client})
    seed_evaluation = calibration_scan_sensor(seed_context)

    late_client = _DatafilesOnlyClient(
        rows_by_type={
            "xrd_calibrant_raw": [
                {
                    "_id": "cal_file_002",
                    "name": "xrd_calibrant_data_000002.h5",
                    "created": "2026-03-11T10:00:00.000+00:00",
                }
            ]
        },
        files_by_item={
            "cal_file_002": [{"_id": "cal_file_002_data", "name": "xrd_calibrant_data_000002.h5"}]
        },
    )
    late_context = build_sensor_context(resources={"GirderClient": late_client}, cursor=seed_evaluation.cursor)
    evaluation = calibration_scan_sensor(late_context)

    assert isinstance(evaluation, SkipReason)


def test_datafiles_failure_falls_back_to_legacy_experiment_discovery(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("DISCOVERY_ALLOW_FALLBACK", "true")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FailingDatafilesWithLegacyClient()})
    evaluation = experiment_folder_sensor(context)

    assert evaluation.run_requests == []
    assert evaluation.cursor
    assert "exp_legacy" in evaluation.cursor


def test_datafiles_failure_falls_back_to_legacy_calibrant_discovery(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("DISCOVERY_ALLOW_FALLBACK", "true")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FailingDatafilesWithLegacyClient()})
    evaluation = calibration_scan_sensor(context)

    assert evaluation.run_requests == []
    assert evaluation.cursor
    assert "legacy_cal_file_1" in evaluation.cursor
