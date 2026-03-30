from MaximaDagster import sensors


class _FakeGirderClient:
    def __init__(self, files_by_item):
        self._files_by_item = files_by_item

    def listItem(self, _folder_id):
        return [{"_id": item_id} for item_id in self._files_by_item.keys()]

    def listFile(self, item_id):
        return self._files_by_item[item_id]


def test_sensor_cursor_roundtrip():
    seen = {"exp_a", "exp_b"}
    cursor = sensors._serialize_seen_folder_ids(seen)

    parsed = sensors._parse_seen_folder_ids(cursor)

    assert parsed == seen


def test_raw_folder_has_scan_files_true():
    gc = _FakeGirderClient(
        files_by_item={
            "item1": [
                {"name": "readme.txt"},
                {"name": "scan_point_0_data_00001.h5"},
            ]
        }
    )

    assert sensors._raw_folder_has_scan_files(gc, "raw") is True


def test_raw_folder_has_scan_files_false():
    gc = _FakeGirderClient(
        files_by_item={
            "item1": [
                {"name": "notes.txt"},
                {"name": "scan_point_0.xrf"},
            ]
        }
    )

    assert sensors._raw_folder_has_scan_files(gc, "raw") is False


def test_calibrant_cursor_roundtrip():
    cursor = sensors._serialize_last_calibrant_file_id("cal_file_42")
    parsed = sensors._parse_last_calibrant_file_id(cursor)
    assert parsed == "cal_file_42"


class _FakeCalibrantGirderClient:
    def listItem(self, _folder_id):
        return [
            {"_id": "item_old", "updated": "2026-03-12T09:00:00.000+00:00"},
            {"_id": "item_new", "updated": "2026-03-12T10:00:00.000+00:00"},
        ]

    def listFile(self, item_id):
        if item_id == "item_old":
            return [{"_id": "cal_1", "name": "xrd_calibrant_data_000001.h5", "updated": "2026-03-12T09:00:00.000+00:00"}]
        if item_id == "item_new":
            return [
                {"_id": "note_1", "name": "notes.txt", "updated": "2026-03-12T10:00:00.000+00:00"},
                {"_id": "cal_2", "name": "xrd_calibrant_data_000002.h5", "updated": "2026-03-12T10:00:00.000+00:00"},
            ]
        return []


def test_latest_calibrant_scan_file_id_picks_newest_scan():
    gc = _FakeCalibrantGirderClient()
    assert sensors._latest_calibrant_scan_file_id(gc, "calibrants_folder") == "cal_2"
