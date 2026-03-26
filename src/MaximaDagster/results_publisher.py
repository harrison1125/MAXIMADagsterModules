"""Helpers for publishing XRD artifacts and metadata to Girder."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


_WORKFLOW_VERSION_CACHE: str | None = None


def get_workflow_version() -> str:
    """Get the workflow version from pyproject.toml.
    
    Returns:
        Version string from pyproject.toml, or a default fallback if unable to read.
    """
    global _WORKFLOW_VERSION_CACHE
    
    if _WORKFLOW_VERSION_CACHE is not None:
        return _WORKFLOW_VERSION_CACHE
    
    try:
        current = Path(__file__).resolve()
        for parent in [current.parent, *current.parent.parents]:
            pyproject_path = parent / "pyproject.toml"
            if pyproject_path.exists():
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)
                    version = data.get("project", {}).get("version", "unknown")
                    _WORKFLOW_VERSION_CACHE = version
                    return version
        
        _WORKFLOW_VERSION_CACHE = "unknown"
        return "unknown"
    except Exception:
        _WORKFLOW_VERSION_CACHE = "unknown"
        return "unknown"


def upload_result_batch(
    gc: Any,
    folder_id: str,
    results: dict[int, pd.DataFrame],
    scan_metadata_getter: callable[[int], dict[str, Any]],
    result_type: str,
    upload_fn: callable,
) -> list[str]:
    """
    Upload a batch of dataframe results as CSV files to Girder.
    
    Handles the common pattern of uploading multiple scan results with optional metadata.
    Deduplicates logic used for azimuthal, lattice, and other scan result types.
    
    Args:
        gc: Girder client instance
        folder_id: Target folder ID in Girder
        results: Dict mapping scan_id (int) to pandas DataFrame
        scan_metadata_getter: Callable(scan_id) -> dict with metadata like {"igsn": "..."}
        result_type: Name suffix for filename (e.g., "azimuthal", "lattice_parameters")
        upload_fn: Function(gc, folder_id, filename, payload, mime_type, metadata) to handle upload
    
    Returns:
        List of uploaded filenames
    """
    uploaded_files = []

    for scan_id, dataframe in results.items():
        filename = f"scan_point_{int(scan_id)}_{result_type}.csv"
        payload = dataframe.to_csv(index=False).encode("utf-8")
        
        item_metadata = scan_metadata_getter(scan_id)
        
        upload_fn(
            gc=gc,
            folder_id=folder_id,
            filename=filename,
            payload=payload,
            mime_type="text/csv",
            item_metadata=item_metadata or None,
        )
        
        uploaded_files.append(filename)

    return uploaded_files


def build_item_link(girder_url: str, item_id: str) -> str:
    base = str(girder_url or "").rstrip("/")
    if base.endswith("/api/v1"):
        base = base[: -len("/api/v1")]
    return f"{base}/#item/{item_id}"


def build_prov_metadata(run_id: str | None) -> dict[str, Any]:
    return {
        "workflow_version": get_workflow_version(),
        "run_id": run_id,
        "time": datetime.now(timezone.utc).isoformat(),
    }


def build_model_metadata(
    model_version: str,
    model_item_id: str,
    girder_url: str,
) -> dict[str, Any]:
    return {
        "version": model_version,
        "item_id": model_item_id,
        "link": build_item_link(girder_url, model_item_id),
    }


def build_calibrant_metadata(
    calibrant_item_id: str,
    girder_url: str,
    igsn: str | None,
) -> dict[str, Any]:
    payload = {
        "item_id": calibrant_item_id,
        "link": build_item_link(girder_url, calibrant_item_id),
    }
    if igsn:
        payload["igsn"] = igsn
    return payload


@dataclass(frozen=True)
class PoniGeometryValues:
    dist: float
    poni1: float
    poni2: float
    rot1: float
    rot2: float
    rot3: float


def extract_poni_geometry_values(geometry: Any) -> PoniGeometryValues:
    return PoniGeometryValues(
        dist=float(getattr(geometry, "dist")),
        poni1=float(getattr(geometry, "poni1")),
        poni2=float(getattr(geometry, "poni2")),
        rot1=float(getattr(geometry, "rot1")),
        rot2=float(getattr(geometry, "rot2")),
        rot3=float(getattr(geometry, "rot3")),
    )


def build_poni_linkage_metadata(
    poni_item_id: str,
    girder_url: str,
    geometry: Any,
) -> dict[str, Any]:
    geometry_values = extract_poni_geometry_values(geometry)
    return {
        "item_id": poni_item_id,
        "link": build_item_link(girder_url, poni_item_id),
        "geometry": {
            "dist": geometry_values.dist,
            "poni1": geometry_values.poni1,
            "poni2": geometry_values.poni2,
            "rot1": geometry_values.rot1,
            "rot2": geometry_values.rot2,
            "rot3": geometry_values.rot3,
        },
    }


__all__ = [
    "upload_result_batch",
    "build_item_link",
    "build_prov_metadata",
    "build_model_metadata",
    "build_calibrant_metadata",
    "extract_poni_geometry_values",
    "build_poni_linkage_metadata",
]
