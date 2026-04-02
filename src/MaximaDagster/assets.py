import io
import os
import tempfile
from pathlib import Path
from typing import Any

import h5py
from dagster import (
    AssetExecutionContext,
    Config,
    DagsterRunStatus,
    RetryRequested,
    RunsFilter,
    asset,
)
from dagster._core.errors import DagsterInvalidPropertyError
from pyFAI.geometry import Geometry

from .utils.girder_helpers import find_child_folder_by_name, get_optional_igsn
from .modules import (
    AzimuthalIntegrator,
    ConverterXRFtoMCA,
    MCAtoFit,
    concentrations as concentrations_module,
)
from .utils.patterns import CALIBRANT_SCAN_PATTERN, H5_SCAN_PATTERN, XRF_SCAN_PATTERN
from .utils.poni_manager import CalibrationCache, load_geometry_from_poni
from .utils.results_publisher import (
    build_calibrant_metadata,
    build_model_metadata,
    build_poni_linkage_metadata,
    build_prov_metadata,
    upload_result_batch,
)
from .sensors import experiment_partitions



def _require_partition_key(context: AssetExecutionContext) -> str:
    if not context.has_partition_key:
        raise ValueError("This asset requires a partition key equal to a Girder experiment folder id.")
    return str(context.partition_key)


def _read_xrd_h5_from_file_id(gc: Any, file_id: str) -> Any:
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, f"{file_id}.h5")
        gc.downloadFile(file_id, local_path)
        with h5py.File(local_path, "r") as h5f:
            return h5f["entry/data/data"][:][0]


def _resolve_model_file(gc: Any, model_item_id: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    """
    Retrieve the .pth calibration model file from a Girder item.
    
    Args:
        gc: Girder client instance
        model_item_id: Girder item ID containing the .pth model file
    
    Returns:
        Tuple of (file_info, metadata, file_name)
    
    Raises:
        ValueError: If no .pth files found or file type invalid
    """
    item_info = gc.getItem(model_item_id)
    meta = item_info.get("meta") or {}
    
    item_files = list(gc.listFile(item_info["_id"]))
    pth_files = [f for f in item_files if str(f.get("name", "")).lower().endswith(".pth")]
    
    if not pth_files:
        raise ValueError(
            f"No .pth files found in Girder item {model_item_id}. "
            f"Expected .pth model file but found: {[f.get('name') for f in item_files]}"
        )
    
    file_info = pth_files[0]
    file_name = file_info["name"]
    
    if not file_name.lower().endswith(".pth"):
        raise ValueError(f"Expected a .pth model file, got {file_name}")
    
    return file_info, meta, file_name


def _latest_calibrant_scan(gc: Any, calibrants_folder_id: str, include_xrd: bool = False) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []

    for item in gc.listItem(calibrants_folder_id):
        item_updated = str(item.get("updated") or "")
        for file_obj in gc.listFile(item["_id"]):
            file_name = str(file_obj.get("name", ""))
            if not CALIBRANT_SCAN_PATTERN.match(file_name):
                continue
            candidates.append(
                {
                    "file_id": str(file_obj["_id"]),
                    "item_id": str(item["_id"]),
                    "file_name": file_name,
                    "updated": str(file_obj.get("updated") or item_updated),
                }
            )

    if not candidates:
        raise ValueError("No calibrant scan .h5 files were found in GIRDER_CALIBRANTS_FOLDER_ID.")

    latest = max(candidates, key=lambda c: (c["updated"], c["file_id"]))
    if include_xrd:
        latest["xrd"] = _read_xrd_h5_from_file_id(gc, latest["file_id"])
    return latest


def _build_calibrator(model_path: str, metadata: dict[str, Any]):
    from .modules import calibrate

    return calibrate.MaximaCalibrator(
        model_path,
        str(metadata["calibrant"]),
        str(metadata["detector"]),
        float(metadata["energy"]),
    )


def _get_current_run_id(context: AssetExecutionContext) -> str | None:
    try:
        return str(context.run.run_id)
    except (AttributeError, DagsterInvalidPropertyError):
        fallback_run_id = vars(context).get("run_id")
        if fallback_run_id is None:
            return None
        return str(fallback_run_id)


def _has_inflight_calibration_precompute(
    context: AssetExecutionContext,
    calibrant_file_id: str,
) -> bool:
    active_statuses = [
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.QUEUED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
    ]
    records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name="calibration_precompute",
            statuses=active_statuses,
            tags={"calibrant_scan_file_id": calibrant_file_id},
        ),
        limit=5,
    )
    current_run_id = _get_current_run_id(context)
    if current_run_id is None:
        return bool(records)
    return any(record.dagster_run.run_id != current_run_id for record in records)


def _ensure_cached_poni(
    context: AssetExecutionContext,
    gc: Any,
    calibration_model_payload: dict[str, Any],
) -> dict[str, Any]:
    """Orchestrate PONI cache lookup or calibration.
    
    Checks cache for existing PONI, handles inflight calibration jobs,
    or triggers fresh calibration if needed.
    """
    calibrants_folder_id = os.getenv("GIRDER_CALIBRANTS_FOLDER_ID")
    if not calibrants_folder_id:
        raise ValueError("GIRDER_CALIBRANTS_FOLDER_ID is not configured.")

    latest_calibrant = _latest_calibrant_scan(gc, calibrants_folder_id)
    calibrant_file_id = latest_calibrant["file_id"]

    cache = CalibrationCache()

    model_metadata = calibration_model_payload["metadata"]
    expected_model_version = str(model_metadata["version"])
    expected_model_file_id = str(model_metadata["source_file_id"])

    cache_entry = cache.get_entry_for_calibrant(
        calibrant_file_id, expected_model_version, expected_model_file_id
    )

    if cache_entry:
        geometry = load_geometry_from_poni(cache_entry.poni_path)
        cache_hit = True
        poni_path = str(cache_entry.poni_path)
    else:
        # If a matching precompute run is already working on this calibrant,
        # wait for it and retry instead of duplicating expensive calibration.
        if _has_inflight_calibration_precompute(context, calibrant_file_id):
            raise RetryRequested(max_retries=10, seconds_to_wait=60)


        latest_xrd = _read_xrd_h5_from_file_id(gc, calibrant_file_id)
        calibrator = _build_calibrator(calibration_model_payload["model_path"], model_metadata)
        poni_path = str(cache.cache_dir / f"{calibrant_file_id}.poni")
        geometry = calibrator.calibrate(latest_xrd, output_path=poni_path)
        cache_hit = False

 
        cache.save_entry(
            calibrant_file_id=calibrant_file_id,
            poni_path=Path(poni_path),
            calibrant_scan_file_name=latest_calibrant["file_name"],
            calibrant_scan_updated=latest_calibrant["updated"],
            model_version=expected_model_version,
            model_source_file_id=expected_model_file_id,
        )

    return {
        "geometry": geometry,
        "poni_path": poni_path,
        "calibrant_scan_file_id": calibrant_file_id,
        "calibrant_scan_item_id": latest_calibrant["item_id"],
        "calibrant_scan_file_name": latest_calibrant["file_name"],
        "cache_hit": cache_hit,
    }


def _upload_bytes_as_item_file(
    gc: Any,
    folder_id: str,
    filename: str,
    payload: bytes,
    mime_type: str,
    item_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = gc.loadOrCreateItem(filename, folder_id)
    existing_files = gc.get(f"item/{item['_id']}/files", parameters={"limit": 1})

    stream = io.BytesIO(payload)
    size = len(payload)

    if existing_files:
        uploaded = gc.uploadFileContents(existing_files[0]["_id"], stream, size)
        if item_metadata:
            gc.addMetadataToItem(item["_id"], item_metadata)
        return uploaded

    file_meta = gc.post(
        "file",
        parameters={
            "parentType": "item",
            "parentId": item["_id"],
            "name": filename,
            "size": size,
            "mimeType": mime_type,
        },
    )
    uploaded = gc._uploadContents(file_meta, stream, size)
    if item_metadata:
        gc.addMetadataToItem(item["_id"], item_metadata)
    return uploaded


def _resolve_model_item_id(gc: Any, model_metadata: dict[str, Any]) -> str:
    configured_item_id = str(os.getenv("GIRDER_MODEL_ITEM_ID") or "").strip()
    if configured_item_id:
        return configured_item_id

    source_file_id = str(model_metadata.get("source_file_id") or "").strip()
    if not source_file_id:
        raise ValueError("Unable to determine model item id: source_file_id is missing.")

    source_file = gc.getFile(source_file_id)
    item_id = str(source_file.get("itemId") or "").strip()
    if not item_id:
        raise ValueError(f"Unable to determine model item id from source file {source_file_id}.")
    return item_id


def _resolve_calibrant_item_igsn(gc: Any, calibrant_item_id: str) -> str | None:
    calibrant_item = gc.getItem(calibrant_item_id)
    return get_optional_igsn(calibrant_item)


@asset(required_resource_keys={"GirderClient"}, partitions_def=experiment_partitions)
def xrdxrf_scans(context: AssetExecutionContext):
    gc = context.resources.GirderClient
    experiment_folder_id = _require_partition_key(context)

    experiment_folder = gc.getFolder(experiment_folder_id)
    raw_folder = find_child_folder_by_name(gc, experiment_folder_id, "raw")
    if not raw_folder:
        raise ValueError(f"No 'raw' folder found under experiment folder {experiment_folder_id}")

    scans: dict[int, dict[str, Any]] = {}

    for item in gc.listItem(raw_folder["_id"]):
        item_igsn = get_optional_igsn(item)
        for file_obj in gc.listFile(item["_id"]):
            file_name = str(file_obj.get("name", ""))
            h5_match = H5_SCAN_PATTERN.match(file_name)
            xrf_match = XRF_SCAN_PATTERN.match(file_name)
            if not h5_match and not xrf_match:
                continue

            scan_num = int((h5_match or xrf_match).group(1))
            scans.setdefault(scan_num, {})
            if item_igsn:
                existing_igsn = scans[scan_num].get("igsn")
                if existing_igsn and existing_igsn != item_igsn:
                    raise ValueError(f"Conflicting IGSN values found for scan {scan_num}: {existing_igsn} vs {item_igsn}")
                scans[scan_num]["igsn"] = item_igsn

            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, file_name)
                gc.downloadFile(file_obj["_id"], local_path)

                if h5_match:
                    with h5py.File(local_path, "r") as h5f:
                        scans[scan_num]["xrd"] = h5f["entry/data/data"][:][0]
                else:
                    with open(local_path, "r", encoding="utf-8", errors="replace") as xrf_file:
                        scans[scan_num]["xrf"] = xrf_file.read()

            scans[scan_num].setdefault("source_files", []).append(file_name)
            scans[scan_num].setdefault("source_file_ids", []).append(str(file_obj["_id"]))

    context.log.info(f"Loaded {len(scans)} scan(s) from experiment {experiment_folder.get('name', experiment_folder_id)}")
    return {
        "experiment_folder_id": experiment_folder_id,
        "experiment_name": str(experiment_folder.get("name", experiment_folder_id)),
        "raw_folder_id": str(raw_folder["_id"]),
        "scans": scans,
    }


@asset(required_resource_keys={"GirderClient"})
def calibration_model(context: AssetExecutionContext):
    gc = context.resources.GirderClient

    model_item_id = os.getenv("GIRDER_MODEL_ITEM_ID")
    if not model_item_id:
        raise ValueError("GIRDER_MODEL_ITEM_ID is not configured.")

    file_info, meta, file_name = _resolve_model_file(gc, model_item_id)

    meta_fields = meta.get("params") if isinstance(meta.get("params"), dict) else meta
    required_fields = ("calibrant", "detector", "energy", "version")
    missing = [field for field in required_fields if field not in meta_fields or meta_fields[field] in (None, "")]
    if missing:
        raise ValueError(
            f"Girder model metadata is missing required field(s): {missing}. "
            f"Found metadata keys: {sorted(meta_fields.keys())}"
        )

    calibrant = str(meta_fields["calibrant"])
    detector = str(meta_fields["detector"])
    energy = float(meta_fields["energy"])
    version = str(meta_fields["version"])

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
            "calibrant": calibrant,
            "detector": detector,
            "energy": energy,
            "version": version,
        },
    }


class PymcaConfig(Config):
    file_id: str


@asset(required_resource_keys={"GirderClient"})
def pymca_config(context: AssetExecutionContext, config: PymcaConfig):
    gc = context.resources.GirderClient

    file_info = gc.getFile(config.file_id)
    file_name = file_info["name"]

    if not file_name.lower().endswith(".cfg"):
        raise ValueError(f"Expected a .cfg file, got {file_name}")

    local_dir = Path("data") / "configs"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / file_name

    if not local_path.exists():
        gc.downloadFile(config.file_id, str(local_path))
        context.log.info(f"Downloaded PyMCA config to {local_path}")
    else:
        context.log.info(f"Using cached PyMCA config at {local_path}")

    return str(local_path)


@asset(required_resource_keys={"GirderClient"})
def poni(context: AssetExecutionContext, calibration_model):
    gc = context.resources.GirderClient
    result = _ensure_cached_poni(context, gc, calibration_model)

    context.log.info(
        f"Calibration cache {'hit' if result['cache_hit'] else 'miss'} for calibrant scan {result['calibrant_scan_file_name']}"
    )

    return result


@asset(partitions_def=experiment_partitions)
def azimuthal_integration(xrdxrf_scans, poni):
    scan_payload = xrdxrf_scans.get("scans", xrdxrf_scans)
    xrd_scans = {scan_id: scan["xrd"] for scan_id, scan in scan_payload.items() if "xrd" in scan}
    return AzimuthalIntegrator.integrate_dict(xrd_scans, poni["geometry"])


@asset(partitions_def=experiment_partitions)
def lattice_parameters(azimuthal_integration):
    from .modules import LatticeParameters

    return LatticeParameters.process_integrated_dict(azimuthal_integration)


@asset(required_resource_keys={"GirderClient"}, partitions_def=experiment_partitions)
def publish_xrd_results(
    context: AssetExecutionContext,
    xrdxrf_scans,
    calibration_model,
    poni,
    azimuthal_integration,
):
    gc = context.resources.GirderClient

    experiment_folder_id = str(xrdxrf_scans["experiment_folder_id"])
    experiment_name = str(xrdxrf_scans["experiment_name"])
    girder_url = str(os.getenv("GIRDER_API_URL") or "")
    run_id = _get_current_run_id(context)

    uploaded_files: list[str] = []

    poni_file = Path(poni["poni_path"])
    if not poni_file.exists():
        raise ValueError(f"Expected PONI file at {poni_file}, but it was not found.")

    model_metadata = calibration_model["metadata"]
    model_item_id = _resolve_model_item_id(gc, model_metadata)
    calibrant_item_id = str(poni.get("calibrant_scan_item_id") or "").strip()
    if not calibrant_item_id:
        calibrant_file = gc.getFile(poni["calibrant_scan_file_id"])
        calibrant_item_id = str(calibrant_file.get("itemId") or "").strip()
    if not calibrant_item_id:
        raise ValueError("Unable to determine calibrant item id for metadata linkage.")

    calibrant_igsn = _resolve_calibrant_item_igsn(gc, calibrant_item_id)

    poni_metadata = {
        "prov": build_prov_metadata(run_id),
        "model": build_model_metadata(
            model_version=str(model_metadata["version"]),
            model_item_id=model_item_id,
            girder_url=girder_url,
        ),
        "calibrant": build_calibrant_metadata(
            calibrant_item_id=calibrant_item_id,
            girder_url=girder_url,
            igsn=calibrant_igsn,
        ),
        "cache_hit": bool(poni["cache_hit"]),
    }

    _upload_bytes_as_item_file(
        gc=gc,
        folder_id=experiment_folder_id,
        filename=poni_file.name,
        payload=poni_file.read_bytes(),
        mime_type="application/octet-stream",
        item_metadata=poni_metadata,
    )
    uploaded_files.append(poni_file.name)

    poni_item_id = str(gc.loadOrCreateItem(poni_file.name, experiment_folder_id)["_id"])
    azimuthal_base_metadata = {
        "prov": build_prov_metadata(run_id),
        "poni": build_poni_linkage_metadata(
            poni_item_id=poni_item_id,
            girder_url=girder_url,
            geometry=poni["geometry"],
        ),
    }

    def get_scan_metadata(scan_id: int) -> dict[str, Any]:
        metadata = dict(azimuthal_base_metadata)
        igsn = xrdxrf_scans["scans"].get(scan_id, {}).get("igsn")
        if igsn:
            metadata["igsn"] = igsn
        return metadata

    uploaded_files.extend(
        upload_result_batch(
            gc=gc,
            folder_id=experiment_folder_id,
            results=azimuthal_integration,
            scan_metadata_getter=get_scan_metadata,

            result_type="azimuthal",
            upload_fn=_upload_bytes_as_item_file,
        )
    )

    context.log.info(f"Uploaded {len(uploaded_files)} XRD artifact(s) to experiment folder {experiment_name}")

    return {
        "experiment_folder_id": experiment_folder_id,
        "uploaded_files": sorted(uploaded_files),
    }


@asset
def mca(xrdxrf_scans):
    scan_payload = xrdxrf_scans.get("scans", xrdxrf_scans)
    xrf_scans = {scan_id: scan["xrf"] for scan_id, scan in scan_payload.items() if "xrf" in scan}
    return ConverterXRFtoMCA.convert_xrf_to_mca(xrf_scans)


@asset
def xrf_fit(mca, pymca_config):
    return MCAtoFit.run_pymca_batch(mca, pymca_config)


@asset
def concentrations(xrf_fit):
    return concentrations_module.process_pymca_directory(xrf_fit)
