"""
Tests for asset helper functions and orchestration logic.
These tests lock the current behavior before refactoring helper placement.
"""

import tempfile
from types import SimpleNamespace
from pathlib import Path
from typing import Any

import pytest

from MaximaDagster import assets
from MaximaDagster.utils.girder_helpers import get_child_folders, find_child_folder_by_name, get_optional_igsn
from MaximaDagster.utils.patterns import H5_SCAN_PATTERN, CALIBRANT_SCAN_PATTERN


class FakeGirderClient:
    """Mock Girder client for testing folder/file operations."""

    def __init__(self, folder_tree: dict[str, Any] | None = None):
        """
        folder_tree maps folder_id -> {"folders": [...], "items": {...}}
        items map item_id -> {"igsn": str | None, "files": [...]}
        """
        self.folder_tree = folder_tree or {}
        self.downloaded_files = {}

    def getFolder(self, folder_id: str) -> dict[str, Any]:
        """Return folder metadata."""
        return {"_id": folder_id, "name": "test_folder"}

    def get(self, route: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Return child folders by parent ID."""
        if route == "folder":
            parent_id = (parameters or {}).get("parentId")
            if parent_id in self.folder_tree:
                return self.folder_tree[parent_id].get("folders", [])
        return []

    def listItem(self, folder_id: str) -> list[dict[str, Any]]:
        """Return items in folder."""
        if folder_id in self.folder_tree:
            items_dict = self.folder_tree[folder_id].get("items", {})
            return [{"_id": item_id, "meta": items_dict[item_id].get("meta", {})} 
                    for item_id in items_dict.keys()]
        return []

    def listFile(self, item_id: str) -> list[dict[str, Any]]:
        """Return files in item."""
        for folder_data in self.folder_tree.values():
            for item_id_key, item_data in folder_data.get("items", {}).items():
                if item_id_key == item_id:
                    return item_data.get("files", [])
        return []

    def downloadFile(self, file_id: str, local_path: str) -> None:
        """Record file download."""
        self.downloaded_files[file_id] = local_path


def test_get_child_folders_returns_empty_when_not_found():
    """get_child_folders should return empty list for nonexistent parent."""
    gc = FakeGirderClient({})
    result = get_child_folders(gc, "nonexistent_parent")
    assert result == []


def test_get_child_folders_returns_api_rows_without_local_reordering():
    """get_child_folders should return API response rows as-is."""
    gc = FakeGirderClient({
        "parent_id": {
            "folders": [
                {"_id": "child_b", "name": "b_folder"},
                {"_id": "child_a", "name": "a_folder"},
            ],
            "items": {},
        }
    })
    result = get_child_folders(gc, "parent_id")
    assert len(result) == 2
    assert result[0]["_id"] == "child_b"
    assert result[1]["_id"] == "child_a"


def test_get_child_folders_requests_server_side_sorting_parameters() -> None:
    captured: dict[str, Any] = {}

    class _CaptureClient:
        def get(self, route: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            captured["route"] = route
            captured["parameters"] = parameters or {}
            return []

    _ = get_child_folders(_CaptureClient(), "parent_id")

    assert captured["route"] == "folder"
    assert captured["parameters"]["sort"] == "lowerName"
    assert captured["parameters"]["sortdir"] == 1


def test_find_child_folder_by_name_case_insensitive():
    """find_child_folder_by_name should match case-insensitively."""
    gc = FakeGirderClient({
        "parent_id": {
            "folders": [
                {"_id": "raw_folder", "name": "raw"},
            ],
            "items": {},
        }
    })
    result = find_child_folder_by_name(gc, "parent_id", "RAW")
    assert result is not None
    assert result["_id"] == "raw_folder"


def test_find_child_folder_by_name_returns_none_when_not_found():
    """find_child_folder_by_name should return None if folder not found."""
    gc = FakeGirderClient({
        "parent_id": {
            "folders": [
                {"_id": "raw_folder", "name": "raw"},
            ],
            "items": {},
        }
    })
    result = find_child_folder_by_name(gc, "parent_id", "processed")
    assert result is None


def test_get_optional_igsn_returns_none_when_missing():
    """get_optional_igsn should return None if metadata absent."""
    result = get_optional_igsn({})
    assert result is None


def test_get_optional_igsn_returns_value_when_present():
    """get_optional_igsn should return stripped IGSN string."""
    result = get_optional_igsn({"meta": {"igsn": "  IGSN-123  "}})
    assert result == "IGSN-123"


def test_get_optional_igsn_returns_none_for_empty_string():
    """get_optional_igsn should return None if IGSN is empty string."""
    result = get_optional_igsn({"meta": {"igsn": ""}})
    assert result is None


def test_require_partition_key_raises_when_missing():
    """_require_partition_key should raise ValueError if no partition key."""
    context = SimpleNamespace(has_partition_key=False)
    with pytest.raises(ValueError, match="partition key"):
        assets._require_partition_key(context)


def test_require_partition_key_returns_key_when_present():
    """_require_partition_key should return partition key string."""
    context = SimpleNamespace(has_partition_key=True, partition_key="exp_folder_123")
    result = assets._require_partition_key(context)
    assert result == "exp_folder_123"


def test_h5_scan_pattern_matches_expected_files():
    """H5_SCAN_PATTERN should match expected file naming."""
    import re
    assert H5_SCAN_PATTERN.match("scan_point_0_data_00001.h5")
    assert H5_SCAN_PATTERN.match("SCAN_POINT_10_DATA_00002.H5")
    assert not H5_SCAN_PATTERN.match("scan_point_0.h5")
    assert not H5_SCAN_PATTERN.match("data_00001.h5")


def test_calibrant_scan_pattern_matches_expected_files():
    """CALIBRANT_SCAN_PATTERN should match expected calibrant naming."""
    import re
    assert CALIBRANT_SCAN_PATTERN.match("xrd_calibrant_data_000001.h5")
    assert CALIBRANT_SCAN_PATTERN.match("XRD_CALIBRANT_DATA_000042.H5")
    # Extract group(1) for scan ID
    m = CALIBRANT_SCAN_PATTERN.match("xrd_calibrant_data_000042.h5")
    assert m.group(1) == "000042"


def test_resolve_model_file_selects_first_pth_and_returns_metadata() -> None:
    class _ModelClient:
        def getItem(self, item_id: str) -> dict[str, Any]:
            assert item_id == "model_item"
            return {
                "_id": "model_item",
                "meta": {"params": {"version": "1.0"}},
            }

        def listFile(self, item_id: str) -> list[dict[str, Any]]:
            assert item_id == "model_item"
            return [
                {"_id": "f_txt", "name": "readme.txt"},
                {"_id": "f_model", "name": "model.pth"},
            ]

    file_info, meta, file_name = assets._resolve_model_file(_ModelClient(), "model_item")

    assert file_info["_id"] == "f_model"
    assert meta["params"]["version"] == "1.0"
    assert file_name == "model.pth"


def test_resolve_model_file_raises_when_no_pth_files_present() -> None:
    class _ModelClient:
        def getItem(self, item_id: str) -> dict[str, Any]:
            assert item_id == "model_item"
            return {"_id": "model_item", "meta": {}}

        def listFile(self, item_id: str) -> list[dict[str, Any]]:
            assert item_id == "model_item"
            return [{"_id": "f_txt", "name": "readme.txt"}]

    with pytest.raises(ValueError, match="No .pth files"):
        assets._resolve_model_file(_ModelClient(), "model_item")


def test_latest_calibrant_scan_legacy_selects_newest_created_then_id() -> None:
    class _CalClient:
        def listItem(self, folder_id: str) -> list[dict[str, Any]]:
            assert folder_id == "calibrants"
            return [{"_id": "item1", "updated": "2026-03-12T10:00:00.000+00:00"}]

        def listFile(self, item_id: str) -> list[dict[str, Any]]:
            assert item_id == "item1"
            return [
                {"_id": "f1", "name": "xrd_calibrant_data_000001.h5", "updated": "2026-03-12T10:00:00.000+00:00"},
                {"_id": "f2", "name": "xrd_calibrant_data_000002.h5", "updated": "2026-03-13T10:00:00.000+00:00"},
            ]

    latest = assets._latest_calibrant_scan_legacy(_CalClient(), "calibrants")

    assert latest["file_id"] == "f2"
    assert latest["item_id"] == "item1"


def test_latest_calibrant_scan_legacy_raises_when_none_found() -> None:
    class _CalClient:
        def listItem(self, folder_id: str) -> list[dict[str, Any]]:
            _ = folder_id
            return [{"_id": "item1", "updated": "2026-03-12T10:00:00.000+00:00"}]

        def listFile(self, item_id: str) -> list[dict[str, Any]]:
            _ = item_id
            return [{"_id": "f_txt", "name": "notes.txt"}]

    with pytest.raises(ValueError, match="No calibrant scan"):
        assets._latest_calibrant_scan_legacy(_CalClient(), "calibrants")


def test_latest_calibrant_scan_datafiles_maps_candidate(monkeypatch) -> None:
    candidate = SimpleNamespace(
        file_id="cal_file_9",
        item_id="cal_item_9",
        file_name="xrd_calibrant_data_000009.h5",
        created="2026-03-14T10:00:00.000+00:00",
    )
    monkeypatch.setattr(assets, "call_with_retries", lambda fn, gc: candidate)

    result = assets._latest_calibrant_scan_datafiles(gc=object())

    assert result["file_id"] == "cal_file_9"
    assert result["item_id"] == "cal_item_9"


def test_latest_calibrant_scan_datafiles_raises_when_none(monkeypatch) -> None:
    monkeypatch.setattr(assets, "call_with_retries", lambda fn, gc: None)

    with pytest.raises(ValueError, match="No calibrant scan"):
        assets._latest_calibrant_scan_datafiles(gc=object())


def test_latest_calibrant_scan_fallbacks_to_legacy_when_allowed(monkeypatch) -> None:
    monkeypatch.setattr(assets, "get_discovery_backend", lambda: assets.DISCOVERY_BACKEND_DATAFILES)
    monkeypatch.setattr(assets, "should_allow_discovery_fallback", lambda: True)
    monkeypatch.setattr(assets, "_latest_calibrant_scan_datafiles", lambda gc, include_xrd=False: (_ for _ in ()).throw(RuntimeError("fail")))
    monkeypatch.setattr(
        assets,
        "_latest_calibrant_scan_legacy",
        lambda gc, calibrants_folder_id, include_xrd=False: {"file_id": "legacy", "item_id": "item", "file_name": "name.h5", "created": "2026"},
    )

    result = assets._latest_calibrant_scan(gc=object(), calibrants_folder_id="calibrants")

    assert result["file_id"] == "legacy"


def test_resolve_model_item_id_prefers_env_value(monkeypatch) -> None:
    monkeypatch.setenv("GIRDER_MODEL_ITEM_ID", "configured_item")

    item_id = assets._resolve_model_item_id(gc=object(), model_metadata={"source_file_id": "unused"})

    assert item_id == "configured_item"


def test_resolve_model_item_id_uses_source_file_when_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("GIRDER_MODEL_ITEM_ID", raising=False)

    class _Client:
        def getFile(self, file_id: str) -> dict[str, Any]:
            assert file_id == "source_file"
            return {"_id": file_id, "itemId": "derived_item"}

    item_id = assets._resolve_model_item_id(gc=_Client(), model_metadata={"source_file_id": "source_file"})

    assert item_id == "derived_item"


def test_resolve_calibrant_item_igsn_reads_item_metadata() -> None:
    class _Client:
        def getItem(self, item_id: str) -> dict[str, Any]:
            assert item_id == "cal_item"
            return {"_id": "cal_item", "meta": {"igsn": " CAL-IGSN "}}

    assert assets._resolve_calibrant_item_igsn(_Client(), "cal_item") == "CAL-IGSN"
