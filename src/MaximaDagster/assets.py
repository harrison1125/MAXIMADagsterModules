import os
import tempfile
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

import h5py
from dagster import (
    AssetExecutionContext,
    RetryRequested,
    asset,
)

from .utils.discovery import (
    call_with_retries,
    fetch_partition_details,
    get_fname,
    get_item_id,
    get_folder_id,
    get_igsn,
)

from .modules import AzimuthalIntegrator, calibrate
from .utils.patterns import CALIBRANT_SCAN_PATTERN, H5_SCAN_PATTERN
from .utils.poni_manager import CalibrationCache, load_geometry_from_poni
from .utils.results_publisher import (
    build_calibrant_metadata,
    build_model_metadata,
    build_poni_linkage_metadata,
    build_prov_metadata,
    upload_artifact
)

from .sensors import experiment_partitions, calibrant_partitions


def _read_xrd_h5_from_item_id(gc: Any, item_id: str) -> Any:
    item_files = list(gc.listFile(item_id))
    h5_files = [
        f for f in item_files 
        if str(f.get("name", "")).lower().endswith(".h5")
    ]
    
    if not h5_files:
        raise ValueError(
            f"Expected an .h5 file inside item {item_id}, but found: "
            f"{[f.get('name') for f in item_files]}"
        )
        
    file_id = str(h5_files[0]["_id"])

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, f"{item_id}.h5")
        gc.downloadFile(file_id, local_path)
        
        with h5py.File(local_path, "r") as h5f:
            return h5f["entry/data/data"][:][0]


def _resolve_model_file(gc: Any, model_item_id: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    item_info = gc.getItem(model_item_id)
    meta = item_info.get("meta") or {}
    
    item_files = list(gc.listFile(item_info["_id"]))
    pth_files = [f for f in item_files if str(f.get("name", "")).lower().endswith(".pth")]
    
    if not pth_files:
        raise ValueError(f"No .pth files found in Girder item {model_item_id}.")
    
    return pth_files[0], meta, pth_files[0]["name"]


def _get_target_calibrant_file_id(gc: Any, partition_key: str) -> str:
    
    keys = partition_key.split("//")
    if len(keys) != 2:
        raise ValueError(f"Invalid partition key format: {partition_key}. Expected format is IGSN//experiment_date.")
    
    experiment_date_str = keys[-1].strip()
    experiment_date = datetime.fromisoformat(experiment_date_str)


    calibrant_datafiles = call_with_retries(
        gc.get,
        "aimdl/datafiles",
        parameters={
            "dataType": "xrd_calibrant_raw",
            "sort": "created",
            "sortdir": -1,  
            "limit": 10     # may need to be increased if calibration becomes more frequent
        }
    )

    for row in calibrant_datafiles:
        calibrant_date_str = row.get("created")
        calibrant_date = datetime.fromisoformat(calibrant_date_str) if calibrant_date_str else None
        fname = get_fname(row)
        
        if calibrant_date < experiment_date and fname and CALIBRANT_SCAN_PATTERN.match(fname):

            item_id = get_item_id(row)
            
            for file_obj in gc.listFile(item_id):
                return str(file_obj["_id"])
                    
    raise ValueError(
        f"No valid calibrant scans found prior to experiment date: {experiment_date_str}"
    )


@asset(required_resource_keys={"GirderClient"})
def calibration_model(context: AssetExecutionContext):
    """Downloads and caches canonical .pth calibration model."""
    gc = context.resources.GirderClient

    model_item_id = os.getenv("GIRDER_MODEL_ITEM_ID")
    if not model_item_id:
        raise ValueError("GIRDER_MODEL_ITEM_ID is not configured.")

    file_info, meta, file_name = _resolve_model_file(gc, model_item_id)
    meta_fields = meta.get("params") if isinstance(meta.get("params"), dict) else meta

    local_dir = Path("data") / "models"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / file_name

    if not local_path.exists():
        gc.downloadFile(file_info["_id"], str(local_path))
        context.log.info(f"Downloaded calibration model to {local_path}")
    else:
        context.log.info(f"Using cached calibration model at {local_path}")

    return {
        "model_path": str(local_path),
        "metadata": {
            "source_file": file_name,
            "source_file_id": str(file_info["_id"]),
            "calibrant": str(meta_fields["calibrant"]),
            "detector": str(meta_fields["detector"]),
            "energy": float(meta_fields["energy"]),
            "version": str(meta_fields["version"]),
        },
    }


@asset(required_resource_keys={"GirderClient"}, partitions_def=calibrant_partitions)
def xrd_calibrant_raw(context: AssetExecutionContext):
    gc = context.resources.GirderClient
    partition_key = context.partition_key

    details = call_with_retries(
        fetch_partition_details, 
        gc, 
        key=partition_key, 
        data_type="xrd_calibrant_raw"
    )

    for row in details:
        fname = get_fname(row)

        if not fname or not CALIBRANT_SCAN_PATTERN.match(fname):
            continue

        item_id = get_item_id(row)
        igsn = get_igsn(row)
        folder = get_folder_id(row)

    if not item_id:
        raise ValueError(
            f"Could not find a valid calibrant file matching the expected pattern in partition {partition_key}."
        )
    
    calibration_pattern = _read_xrd_h5_from_item_id(gc, item_id)

    context.log.info(f"Successfully loaded raw calibrant scan: {fname}")
    
    return {
        "calibrant_item_id": item_id,
        "calibrant_file_name": fname,
        "pattern": calibration_pattern,
        "igsn": igsn,
        "folder_id": folder,
    }


@asset(required_resource_keys={"GirderClient"}, partitions_def=calibrant_partitions)
def poni(context: AssetExecutionContext, calibration_model, xrd_calibrant_raw):
    gc = context.resources.GirderClient
    run_id = str(context.run.run_id)
    girder_url = str(os.getenv("GIRDER_API_URL") or "")
    partition_key = context.partition_key
    
    calibrant_item_id = xrd_calibrant_raw["calibrant_item_id"]
    calibrant_file_name = xrd_calibrant_raw["calibrant_file_name"]
    calibrant_igsn = xrd_calibrant_raw["igsn"]
    model_metadata = calibration_model["metadata"]

    calibrator = calibrate.MaximaCalibrator(
        calibration_model["model_path"],
        str(model_metadata["calibrant"]),
        str(model_metadata["detector"]),
        float(model_metadata["energy"]),
    )

    cache = CalibrationCache()

    poni_path = str(cache.cache_dir / f"{calibrant_file_name}.poni")                                        #TODO: consider updating this naming convention
    geometry = calibrator.calibrate(xrd_calibrant_raw["pattern"], output_path=poni_path)

    poni_metadata = {
        "prov": build_prov_metadata(run_id),
        "model": build_model_metadata(
            model_version=model_metadata["version"],
            model_item_id=model_metadata["source_file_id"],
            girder_url=girder_url,
        ),
        "calibrant": build_calibrant_metadata(
            calibrant_item_id=calibrant_item_id,
            girder_url=girder_url,
            igsn=calibrant_igsn,
        ),
        "dataType": "xrd_calibrant_derived",
    }

    poni_bytes = Path(poni_path).read_bytes()
    poni_item_id = upload_artifact(
        gc=gc,
        folder_id=xrd_calibrant_raw["folder_id"],
        filename=f"{calibrant_file_name}.poni",
        payload=poni_bytes,
        mime_type="text/plain",
        metadata=poni_metadata,
    )

    cache.save_entry(
        calibrant_file_id=calibrant_item_id,
        poni_path=Path(poni_path),
        poni_item_id=poni_item_id,
        calibrant_scan_file_name=calibrant_file_name,
        calibrant_scan_updated=datetime.now(timezone.utc).isoformat(), 
        model_version=model_metadata["version"],
        model_source_file_id=model_metadata["source_file_id"],
    )

    context.log.info(f"Generated new PONI geometry for {partition_key}")
    return {"poni_path": poni_path, "poni_item_id": poni_item_id}


@asset(required_resource_keys={"GirderClient"}, partitions_def=experiment_partitions)
def xrd_raw(context: AssetExecutionContext):
    gc = context.resources.GirderClient 
    partition_key = context.partition_key

    details = call_with_retries(
        fetch_partition_details, 
        gc, 
        key=partition_key, 
        data_type="xrd_raw"
    )

    scans: dict[int, dict[str, Any]] = {}
    folder_id: str | None = None

    for row in details:
        fname = get_fname(row)
        
        h5_match = H5_SCAN_PATTERN.match(fname)
        if not fname or not h5_match:
            continue

        current_folder_id = get_folder_id(row)
        if folder_id is None:
            folder_id = current_folder_id
        elif folder_id != current_folder_id:
            raise ValueError(
                f"Unexpected pattern: Multiple raw folder IDs found in partition {partition_key} "
                f"({folder_id} and {current_folder_id})."
            )

        item_id = get_item_id(row)
        if not item_id:
            context.log.warning(f"Skipping {fname}: Could not extract item ID.")
            continue

        scan_num = int(h5_match.group(1))
        scans.setdefault(scan_num, {})

        item_igsn = get_igsn(row)
        if item_igsn:
            scans[scan_num]["igsn"] = item_igsn

        scans[scan_num]["xrd"] = _read_xrd_h5_from_item_id(gc, item_id)
        scans[scan_num].setdefault("source_files", []).append(fname)
        scans[scan_num].setdefault("source_item_ids", []).append(item_id)

    if not folder_id:
        raise ValueError(
            f"Could not find a folder containing valid XRD files matching "
            f"the expected pattern in partition {partition_key}."
        )

    raw_folder = gc.getFolder(folder_id)
    
    if raw_folder.get("parentCollection") != "folder":
        raise ValueError(
            f"Raw folder {folder_id} is directly under a {raw_folder.get('parentCollection')}."
        )
        
    experiment_folder_id = str(raw_folder.get("parentId", ""))
    experiment_folder = gc.getFolder(experiment_folder_id)
    experiment_name = str(experiment_folder.get("name", experiment_folder_id))

    context.log.info(
        f"Loaded {len(scans)} scan(s) from experiment {experiment_name}"
    )

    return {
        "experiment_folder_id": experiment_folder_id,
        "partition_key": partition_key,
        "experiment_name": experiment_name,
        "folder_id": folder_id,
        "scans": scans,
    }


@asset(required_resource_keys={"GirderClient"}, partitions_def=experiment_partitions)
def active_poni(context: AssetExecutionContext, calibration_model):
    """Retrieves the correct PONI geometry, waiting if it is currently generating."""
    gc = context.resources.GirderClient
    cache = CalibrationCache()
    
    target_calibrant_id = _get_target_calibrant_file_id(gc, context.partition_key)
    
    model_version = str(calibration_model["metadata"]["version"])
    model_source_id = str(calibration_model["metadata"]["source_file_id"])

    cache_entry = cache.get_entry_for_calibrant(
        target_calibrant_id, model_version, model_source_id
    )
    
    if not cache_entry:
        context.log.info(
            f"PONI for calibrant {target_calibrant_id} is not yet in cache. "
            "Waiting for calibration_precompute to finish..."
        )

        raise RetryRequested(max_retries=30, seconds_to_wait=60)
        
    context.log.info(f"Successfully loaded cached PONI for calibrant {target_calibrant_id}")
    return {
        "geometry": load_geometry_from_poni(cache_entry.poni_path),
        "poni_path": str(cache_entry.poni_path),
        "poni_item_id": cache_entry.poni_item_id,
        "calibrant_scan_file_id": cache_entry.calibrant_scan_file_id,
        "cache_hit": True
    }


@asset(required_resource_keys={"GirderClient"}, partitions_def=experiment_partitions)
def azimuthal_integration(context: AssetExecutionContext, xrd_raw, active_poni):

    gc = context.resources.GirderClient
    run_id = str(context.run.run_id)
    girder_url = str(os.getenv("GIRDER_API_URL") or "")
    
    experiment_folder_id = xrd_raw["experiment_folder_id"]
    scans = xrd_raw["scans"]

    xrd_scans = {scan_id: scan["xrd"] for scan_id, scan in scans.items() if "xrd" in scan}
    results = AzimuthalIntegrator.integrate_dict(xrd_scans, active_poni["geometry"])

    ai_metadata = {
        "prov": build_prov_metadata(run_id),
        "poni": build_poni_linkage_metadata(
            poni_item_id=active_poni["poni_item_id"],
            girder_url=girder_url,
            geometry=active_poni["geometry"],
        ),
        "dataType": "xrd_derived",
    }

    uploaded_files = []

    for scan_id, integration_result in results.items():
        filename = f"scan_point_{int(scan_id)}_azimuthal.csv"
        payload = integration_result.to_csv(index=False).encode("utf-8")
        
        igsn = scans[scan_id].get("igsn")
        item_meta = {**ai_metadata, "igsn": igsn} if igsn else ai_metadata

        upload_artifact(
            gc=gc,
            folder_id=experiment_folder_id,
            filename=filename,
            payload=payload,
            mime_type="text/csv",
            metadata=item_meta,
        )
        
        uploaded_files.append(filename)

    context.log.info(f"Uploaded {len(uploaded_files)} azimuthal integration result(s) to experiment folder {xrd_raw['experiment_name']}")

    return results