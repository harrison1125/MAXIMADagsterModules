import pytest

from MaximaDagster.utils.discovery import (
    call_with_retries,
    get_datafiles_limit,
    get_datafiles_max_pages,
    get_datafiles_max_rows,
    get_datafiles_retry_count,
    get_datafiles_retry_delay_seconds,
    get_discovery_backend,
    latest_calibrant_candidate_from_datafiles,
    list_xrd_scan_candidates_from_datafiles,
    list_experiment_candidates_from_datafiles,
    should_allow_discovery_fallback,
)


class _FakeDatafilesClient:
    def __init__(self, rows_by_type, files_by_item=None):
        self.rows_by_type = rows_by_type
        self.files_by_item = files_by_item or {}

    def get(self, route, parameters=None):
        assert route == "aimdl/datafiles"
        data_type = (parameters or {}).get("dataType")
        limit = int((parameters or {}).get("limit", 200))
        offset = int((parameters or {}).get("offset", 0))
        rows = list(self.rows_by_type.get(data_type, []))
        return rows[offset : offset + limit]

    def listFile(self, item_id):
        return list(self.files_by_item.get(item_id, []))

    def getFolder(self, folder_id):
        if folder_id == "raw_1":
            return {"_id": "raw_1", "parentId": "exp_1", "name": "raw"}
        return {"_id": folder_id, "parentId": "exp_unknown", "name": "raw"}


def test_list_experiment_candidates_deduplicates_by_experiment_id(monkeypatch):
    monkeypatch.setenv("DISCOVERY_DATAFILES_LIMIT", "2")

    gc = _FakeDatafilesClient(
        {
            "xrd_raw": [
                {
                    "_id": "f3",
                    "name": "scan_point_1_data_00003.h5",
                    "created": "2026-01-03T00:00:00.000+00:00",
                    "folderId": "raw_1",
                    "experimentFolderId": "exp_1",
                    "experimentFolderName": "Experiment 1",
                },
                {
                    "_id": "f2",
                    "name": "scan_point_0_data_00002.h5",
                    "created": "2026-01-02T00:00:00.000+00:00",
                    "folderId": "raw_1",
                    "experimentFolderId": "exp_1",
                    "experimentFolderName": "Experiment 1",
                },
                {
                    "_id": "f1",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-01-01T00:00:00.000+00:00",
                    "folderId": "raw_2",
                    "experimentFolderId": "exp_2",
                    "experimentFolderName": "Experiment 2",
                },
            ]
        }
    )

    rows = list_experiment_candidates_from_datafiles(gc)

    assert {row.experiment_folder_id for row in rows} == {"exp_1", "exp_2"}


def test_latest_calibrant_candidate_uses_created_then_file_id(monkeypatch):
    monkeypatch.setenv("DISCOVERY_DATAFILES_LIMIT", "10")

    gc = _FakeDatafilesClient(
        {
            "xrd_calibrant_raw": [
                {
                    "_id": "cal_2",
                    "name": "xrd_calibrant_data_000002.h5",
                    "created": "2026-01-10T00:00:00.000+00:00",
                },
                {
                    "_id": "cal_3",
                    "name": "xrd_calibrant_data_000003.h5",
                    "created": "2026-01-10T00:00:00.000+00:00",
                },
            ]
        },
        files_by_item={
            "cal_2": [{"_id": "cal_file_2", "name": "xrd_calibrant_data_000002.h5"}],
            "cal_3": [{"_id": "cal_file_3", "name": "xrd_calibrant_data_000003.h5"}],
        },
    )

    latest = latest_calibrant_candidate_from_datafiles(gc)

    assert latest is not None
    assert latest.file_id == "cal_file_3"
    assert latest.item_id == "cal_3"


def test_list_xrd_scan_candidates_filters_to_requested_experiment(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_LIMIT", "10")

    gc = _FakeDatafilesClient(
        {
            "xrd_raw": [
                {
                    "_id": "row_1",
                    "name": "scan_point_0_data_00001.h5",
                    "created": "2026-01-03T00:00:00.000+00:00",
                    "folderId": "raw_1",
                    "experimentFolderId": "exp_1",
                    "meta": {"igsn": "IGSN-1"},
                },
                {
                    "_id": "row_2",
                    "name": "scan_point_1_data_00002.h5",
                    "created": "2026-01-04T00:00:00.000+00:00",
                    "folderId": "raw_2",
                    "experimentFolderId": "exp_2",
                },
            ]
        },
        files_by_item={
            "row_1": [{"_id": "file_1", "name": "scan_point_0_data_00001.h5"}],
            "row_2": [{"_id": "file_2", "name": "scan_point_1_data_00002.h5"}],
        },
    )

    candidates = list_xrd_scan_candidates_from_datafiles(gc, "exp_1")

    assert len(candidates) == 1
    assert candidates[0].experiment_folder_id == "exp_1"
    assert candidates[0].file_id == "file_1"
    assert candidates[0].igsn == "IGSN-1"


def test_discovery_backend_and_fallback_env_parsing(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")
    monkeypatch.setenv("DISCOVERY_ALLOW_FALLBACK", "yes")

    assert get_discovery_backend() == "datafiles"
    assert should_allow_discovery_fallback() is True

    monkeypatch.setenv("DISCOVERY_BACKEND", "unknown")
    monkeypatch.setenv("DISCOVERY_ALLOW_FALLBACK", "off")

    assert get_discovery_backend() == "legacy"
    assert should_allow_discovery_fallback() is False


def test_discovery_limit_and_retry_parsing_with_invalid_values(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_LIMIT", "abc")
    monkeypatch.setenv("DISCOVERY_DATAFILES_MAX_PAGES", "-1")
    monkeypatch.setenv("DISCOVERY_DATAFILES_MAX_ROWS", "0")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "999")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "999")

    assert get_datafiles_limit() == 200
    assert get_datafiles_max_pages() == 0
    assert get_datafiles_max_rows() == 0
    assert get_datafiles_retry_count() == 5
    assert get_datafiles_retry_delay_seconds() == 10.0


def test_call_with_retries_retries_then_succeeds(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "2")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")

    state = {"attempts": 0}

    def flaky():
        state["attempts"] += 1
        if state["attempts"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert call_with_retries(flaky) == "ok"
    assert state["attempts"] == 3


def test_call_with_retries_raises_last_error_after_exhaustion(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "1")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")

    state = {"attempts": 0}

    def always_fail():
        state["attempts"] += 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        call_with_retries(always_fail)

    assert state["attempts"] == 2
