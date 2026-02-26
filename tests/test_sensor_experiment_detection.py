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
