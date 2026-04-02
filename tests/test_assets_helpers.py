"""
Tests for asset helper functions and orchestration logic.
These tests lock the current behavior before refactoring helper placement.
"""

import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from MaximaDagster import assets
from MaximaDagster.utils.girder_helpers import get_child_folders, find_child_folder_by_name, get_optional_igsn
from MaximaDagster.utils.patterns import H5_SCAN_PATTERN, CALIBRANT_SCAN_PATTERN, XRF_SCAN_PATTERN


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


def test_get_child_folders_returns_children():
    """get_child_folders should return child folders sorted by name."""
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
    assert result[0]["_id"] == "child_b"  # sorted order depends on Girder API response


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


def test_xrf_scan_pattern_matches_expected_files():
    """XRF_SCAN_PATTERN should match expected XRF naming."""
    import re
    assert XRF_SCAN_PATTERN.match("scan_point_0.xrf")
    assert XRF_SCAN_PATTERN.match("SCAN_POINT_42.XRF")
    assert not XRF_SCAN_PATTERN.match("scan_point_0_data.xrf")
