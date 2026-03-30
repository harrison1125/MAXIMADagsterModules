"""
Shared Girder client utilities for folder/item/file operations.
Consolidates helpers used across assets.py and sensors.py to reduce duplication.
"""

from typing import Any


def get_child_folders(gc: Any, parent_folder_id: str) -> list[dict[str, Any]]:
    """
    Retrieve child folders within a parent folder, sorted by name.
    
    Args:
        gc: Girder client instance
        parent_folder_id: ID of parent folder
    
    Returns:
        List of child folder metadata dicts (possibly empty)
    """
    folders = gc.get(
        "folder",
        parameters={
            "parentType": "folder",
            "parentId": parent_folder_id,
            "sort": "lowerName",
            "sortdir": 1,
            "limit": 0,
        },
    )
    return folders or []


def find_child_folder_by_name(
    gc: Any, parent_folder_id: str, name: str
) -> dict[str, Any] | None:
    """
    Find a child folder by name (case-insensitive) within a parent folder.
    
    Args:
        gc: Girder client instance
        parent_folder_id: ID of parent folder
        name: Exact name to search for (case-insensitive)
    
    Returns:
        Folder metadata dict if found, None otherwise
    """
    for folder in get_child_folders(gc, parent_folder_id):
        if folder.get("name", "").lower() == name.lower():
            return folder
    return None


def get_optional_igsn(item: dict[str, Any]) -> str | None:
    """
    Extract IGSN from item metadata if present and non-empty.
    
    Args:
        item: Girder item metadata dict
    
    Returns:
        Trimmed IGSN string if present and non-empty, None otherwise
    """
    meta = item.get("meta")
    if not isinstance(meta, dict):
        return None

    igsn = meta.get("igsn")
    if isinstance(igsn, str) and igsn.strip():
        return igsn.strip()
    return None


__all__ = [
    "get_child_folders",
    "find_child_folder_by_name",
    "get_optional_igsn",
]
