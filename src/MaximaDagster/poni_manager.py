"""
Module for managing PONI (calibration) file lifecycle and caching.
Handles cache index I/O, validation, and orchestration of calibrator execution.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyFAI.geometry import Geometry
from pyFAI.integrator.azimuthal import AzimuthalIntegrator as PyFAIAzimuthalIntegrator


@dataclass(frozen=True)
class CacheEntry:
    """Represents a validated cache entry for a calibrant scan."""

    poni_path: Path
    """Path to cached PONI file."""

    calibrant_scan_file_id: str
    """Girder file ID of calibrant scan that generated this PONI."""

    calibrant_scan_file_name: str
    """Filename of calibrant scan."""

    calibrant_scan_updated: str
    """ISO timestamp when calibrant scan was last updated."""

    model_version: str
    """Model version used for calibration."""

    model_source_file_id: str
    """Girder file ID of the model used."""

    updated_at: str
    """ISO timestamp when cache entry was created."""


class CalibrationCache:
    """Manages PONI cache index and validity checking."""

    def __init__(self, cache_dir: Path | None = None):
        """
        Initialize cache manager.
        
        Args:
            cache_dir: Directory for cache files (default: data/calibrations)
        """
        self.cache_dir = cache_dir or Path("data") / "calibrations"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.cache_dir / "index.json"

    def load_index(self) -> dict[str, dict[str, Any]]:
        """Load cache index from disk."""
        if not self.index_path.exists():
            return {}
        return json.loads(self.index_path.read_text(encoding="utf-8"))

    def save_index(self, index: dict[str, dict[str, Any]]) -> None:
        """Persist cache index to disk."""
        self.index_path.write_text(
            json.dumps(index, indent=2, sort_keys=True), encoding="utf-8"
        )

    def validate_entry(
        self,
        entry: dict[str, Any],
        expected_model_version: str,
        expected_model_file_id: str,
    ) -> bool:
        """
        Check if cache entry is valid (file exists, versions match).
        
        Args:
            entry: Cache index entry dict
            expected_model_version: Model version to expect
            expected_model_file_id: Model file ID to expect
        
        Returns:
            True if cache entry is valid and usable, False otherwise
        """
        return bool(
            entry
            and Path(str(entry.get("poni_path", ""))).exists()
            and str(entry.get("model_version", "")) == expected_model_version
            and str(entry.get("model_source_file_id", "")) == expected_model_file_id
        )

    def get_entry_for_calibrant(
        self,
        calibrant_file_id: str,
        expected_model_version: str,
        expected_model_file_id: str,
    ) -> CacheEntry | None:
        """
        Retrieve a valid cache entry if it exists and is valid.
        
        Args:
            calibrant_file_id: ID of calibrant scan
            expected_model_version: Expected model version
            expected_model_file_id: Expected model file ID
        
        Returns:
            CacheEntry if valid entry found, None otherwise
        """
        index = self.load_index()
        entry = index.get(calibrant_file_id)
        
        if not self.validate_entry(entry, expected_model_version, expected_model_file_id):
            return None
        
        return CacheEntry(
            poni_path=Path(str(entry["poni_path"])),
            calibrant_scan_file_id=entry["calibrant_scan_file_id"],
            calibrant_scan_file_name=entry["calibrant_scan_file_name"],
            calibrant_scan_updated=entry["calibrant_scan_updated"],
            model_version=entry["model_version"],
            model_source_file_id=entry["model_source_file_id"],
            updated_at=entry["updated_at"],
        )

    def save_entry(
        self,
        calibrant_file_id: str,
        poni_path: Path,
        calibrant_scan_file_name: str,
        calibrant_scan_updated: str,
        model_version: str,
        model_source_file_id: str,
    ) -> None:
        """
        Store a new cache entry in the index.
        
        Args:
            calibrant_file_id: ID of calibrant scan
            poni_path: Path to PONI file
            calibrant_scan_file_name: Filename of calibrant scan
            calibrant_scan_updated: ISO timestamp of calibrant update
            model_version: Version of model used
            model_source_file_id: Girder file ID of model
        """
        index = self.load_index()
        index[calibrant_file_id] = {
            "poni_path": str(poni_path),
            "calibrant_scan_file_id": calibrant_file_id,
            "calibrant_scan_file_name": calibrant_scan_file_name,
            "calibrant_scan_updated": calibrant_scan_updated,
            "model_version": model_version,
            "model_source_file_id": model_source_file_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save_index(index)


def load_geometry_from_poni(poni_path: Path | str) -> Geometry:
    """
    Load Geometry object from PONI file.
    
    Args:
        poni_path: Path to PONI file
    
    Returns:
        Geometry object with calibration parameters
    """
    ai = PyFAIAzimuthalIntegrator()
    ai.load(str(poni_path))
    return Geometry(
        dist=ai.dist,
        poni1=ai.poni1,
        poni2=ai.poni2,
        rot1=ai.rot1,
        rot2=ai.rot2,
        rot3=ai.rot3,
        detector=ai.detector,
        wavelength=ai.wavelength,
    )


__all__ = [
    "CacheEntry",
    "CalibrationCache",
    "load_geometry_from_poni",
]
