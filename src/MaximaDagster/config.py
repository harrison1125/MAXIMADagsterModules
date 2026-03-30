"""
Centralized configuration for Girder integration and experiment workflows.
Also defines typed payload contracts for asset hand-offs.
Consolidates environment variable handling with typed, validated contracts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TypedDict
import pandas as pd
from pyFAI.geometry import Geometry


@dataclass(frozen=True)
class GirderFolderConfig:
    """Configuration for Girder folder IDs used across assets and sensors."""
    
    root_folder_id: str
    """ID of root folder containing experiment subfolders."""
    
    calibrants_folder_id: str
    """ID of folder containing calibrant .h5 scan files."""
    
    model_item_id: str
    """Girder item ID containing the .pth calibration model file."""


class ScanMetadata(TypedDict, total=False):
    """Metadata associated with a single XRD/XRF scan point."""
    igsn: str
    """Generic sample number for sample identification."""


class XrdXrfScansPayload(TypedDict):
    """Output payload from xrdxrf_scans asset."""
    experiment_folder_id: str
    experiment_name: str
    raw_folder_id: str
    scans: dict[int, dict[str, any]]


class CalibrationModelPayload(TypedDict):
    """Output payload from calibration_model asset."""
    model_path: str
    metadata: dict[str, str | float]


class PoniPayload(TypedDict):
    """Output payload from poni asset."""
    geometry: Geometry
    poni_path: str
    calibrant_scan_file_id: str
    calibrant_scan_item_id: str
    calibrant_scan_file_name: str
    cache_hit: bool


def load_girder_folder_config(
    root_env: str = "GIRDER_ROOT_FOLDER_ID",
    calibrants_env: str = "GIRDER_CALIBRANTS_FOLDER_ID",
    model_env: str = "GIRDER_MODEL_ITEM_ID",
) -> GirderFolderConfig:
    """
    Load Girder folder configuration from environment variables.
    
    Args:
        root_env: Environment variable name for root folder ID (default: GIRDER_ROOT_FOLDER_ID)
        calibrants_env: Environment variable name for calibrants folder ID (default: GIRDER_CALIBRANTS_FOLDER_ID)
        model_env: Environment variable name for model item ID (default: GIRDER_MODEL_ITEM_ID)
    
    Returns:
        Populated GirderFolderConfig
    
    Raises:
        ValueError: If any required environment variable is missing
    """
    root_folder_id = os.getenv(root_env, "").strip()
    calibrants_folder_id = os.getenv(calibrants_env, "").strip()
    model_item_id = os.getenv(model_env, "").strip()
    
    missing = [
        env
        for env, value in [
            (root_env, root_folder_id),
            (calibrants_env, calibrants_folder_id),
            (model_env, model_item_id),
        ]
        if not value
    ]
    
    if missing:
        raise ValueError(
            f"Missing required Girder configuration environment variables: {', '.join(missing)}"
        )
    
    return GirderFolderConfig(
        root_folder_id=root_folder_id,
        calibrants_folder_id=calibrants_folder_id,
        model_item_id=model_item_id,
    )


__all__ = [
    "GirderFolderConfig",
    "load_girder_folder_config",
    "ScanMetadata",
    "XrdXrfScansPayload",
    "CalibrationModelPayload",
    "PoniPayload",
]
