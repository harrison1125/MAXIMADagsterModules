from dagster import build_sensor_context

from MaximaDagster.sensors import calibration_scan_sensor, experiment_folder_sensor


class _DatafilesOnlyClient:
    def __init__(self):
        self.calls = []

    def get(self, route, parameters=None):
        self.calls.append((route, parameters or {}))
        if route != "aimdl/datafiles":
            raise AssertionError(f"Unexpected route {route}")

        data_type = (parameters or {}).get("dataType")
        offset = (parameters or {}).get("offset", 0)
        if offset != 0:
            return []

        if data_type == "xrd_raw":
            return [
                {
                    "_id": "file_001",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                    "folderId": "raw_01",
                    "experimentFolderId": "exp_01",
                    "experimentFolderName": "exp one",
                }
            ]

        if data_type == "xrd_calibrant_raw":
            return [
                {
                    "_id": "cal_file_001",
                    "name": "xrd_calibrant_data_000001.h5",
                    "created": "2026-03-12T10:00:00.000+00:00",
                }
            ]

        return []

    def getFolder(self, folder_id):
        if folder_id == "raw_01":
            return {"_id": "raw_01", "parentId": "exp_01", "name": "raw"}
        if folder_id == "exp_01":
            return {"_id": "exp_01", "name": "exp one"}
        raise Exception(f"Unknown folder {folder_id}")


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

    context = build_sensor_context(resources={"GirderClient": _DatafilesOnlyClient()})
    evaluation = experiment_folder_sensor(context)

    assert len(evaluation.run_requests) == 1
    run_request = evaluation.run_requests[0]
    assert run_request.run_key == "experiment:exp_01"
    assert run_request.partition_key == "exp_01"


def test_calibration_sensor_uses_datafiles_backend(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _DatafilesOnlyClient()})
    evaluation = calibration_scan_sensor(context)

    assert len(evaluation.run_requests) == 1
    run_request = evaluation.run_requests[0]
    assert run_request.run_key == "calibrant:cal_file_001"
    assert run_request.tags["calibrant_scan_file_id"] == "cal_file_001"


def test_datafiles_failure_falls_back_to_legacy_experiment_discovery(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("DISCOVERY_ALLOW_FALLBACK", "true")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("GIRDER_ROOT_FOLDER_ID", "root_folder")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FailingDatafilesWithLegacyClient()})
    evaluation = experiment_folder_sensor(context)

    assert len(evaluation.run_requests) == 1
    run_request = evaluation.run_requests[0]
    assert run_request.run_key == "experiment:exp_legacy"
    assert run_request.partition_key == "exp_legacy"


def test_datafiles_failure_falls_back_to_legacy_calibrant_discovery(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("DISCOVERY_ALLOW_FALLBACK", "true")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("GIRDER_CALIBRANTS_FOLDER_ID", "calibrants_folder")

    context = build_sensor_context(resources={"GirderClient": _FailingDatafilesWithLegacyClient()})
    evaluation = calibration_scan_sensor(context)

    assert len(evaluation.run_requests) == 1
    run_request = evaluation.run_requests[0]
    assert run_request.run_key == "calibrant:legacy_cal_file_1"
    assert run_request.tags["calibrant_scan_file_id"] == "legacy_cal_file_1"
