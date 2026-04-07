from MaximaDagster.utils.discovery import (
    latest_calibrant_candidate_from_datafiles,
    list_experiment_candidates_from_datafiles,
)


class _FakeDatafilesClient:
    def __init__(self, rows_by_type):
        self.rows_by_type = rows_by_type

    def get(self, route, parameters=None):
        assert route == "aimdl/datafiles"
        data_type = (parameters or {}).get("dataType")
        limit = int((parameters or {}).get("limit", 200))
        offset = int((parameters or {}).get("offset", 0))
        rows = list(self.rows_by_type.get(data_type, []))
        return rows[offset : offset + limit]


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
        }
    )

    latest = latest_calibrant_candidate_from_datafiles(gc)

    assert latest is not None
    assert latest.file_id == "cal_3"
    assert latest.item_id == "cal_3"
