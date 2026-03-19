"""
Module for managing result publication to Girder.
Handles batch uploads of analysis results (CSVs, manifests) and deduplicates upload logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


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


def build_run_manifest(
    experiment_folder_id: str,
    experiment_name: str,
    partition_key: str | None,
    model_metadata: dict[str, Any],
    calibration_metadata: dict[str, Any],
    run_id: str | None,
    artifacts: list[str],
) -> dict[str, Any]:
    """
    Assemble the run manifest document.
    
    Args:
        experiment_folder_id: Girder experiment folder ID
        experiment_name: Human-readable experiment name
        partition_key: Partition key if partitioned, None otherwise
        model_metadata: Model metadata dict
        calibration_metadata: Calibration metadata (cache hit, poni filename, etc.)
        run_id: Dagster run ID
        artifacts: List of artifact filenames uploaded
    
    Returns:
        Dictionary compatible with JSON serialization
    """
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "experiment": {
            "folder_id": experiment_folder_id,
            "name": experiment_name,
            "partition_key": partition_key,
        },
        "model": model_metadata,
        "calibration": calibration_metadata,
        "artifacts": sorted(artifacts),
    }


__all__ = [
    "upload_result_batch",
    "build_run_manifest",
]
