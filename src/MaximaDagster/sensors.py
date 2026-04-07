import json
import os
from typing import Any

from dagster import (
    DynamicPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from .utils.discovery import (
    DISCOVERY_BACKEND_DATAFILES,
    call_with_retries,
    get_discovery_backend,
    latest_calibrant_candidate_from_datafiles,
    list_experiment_candidates_from_datafiles,
    should_allow_discovery_fallback,
)
from .utils.girder_helpers import find_child_folder_by_name, get_child_folders
from .utils.patterns import CALIBRANT_SCAN_PATTERN, H5_SCAN_PATTERN


experiment_partitions = DynamicPartitionsDefinition(name="experiments")

# Compatibility shims for internal sensor functions
_get_child_folders = get_child_folders
_find_child_folder_by_name = find_child_folder_by_name
_H5_SCAN_PATTERN = H5_SCAN_PATTERN
_CALIBRANT_SCAN_PATTERN = CALIBRANT_SCAN_PATTERN


def _raw_folder_has_scan_files(gc: Any, raw_folder_id: str) -> bool:
    for item in gc.listItem(raw_folder_id):
        for file_obj in gc.listFile(item["_id"]):
            if _H5_SCAN_PATTERN.match(file_obj.get("name", "")):
                return True
    return False


def _latest_calibrant_scan_file_id(gc: Any, calibrants_folder_id: str) -> str | None:
    latest: tuple[str, str] | None = None

    for item in gc.listItem(calibrants_folder_id):
        item_updated = str(item.get("updated") or "")
        for file_obj in gc.listFile(item["_id"]):
            file_name = str(file_obj.get("name", ""))
            if not _CALIBRANT_SCAN_PATTERN.match(file_name):
                continue

            file_id = str(file_obj.get("_id", ""))
            if not file_id:
                continue

            updated = str(file_obj.get("updated") or item_updated)
            candidate = (updated, file_id)
            if latest is None or candidate > latest:
                latest = candidate

    if latest is None:
        return None
    return latest[1]


def _parse_seen_folder_ids(cursor: str | None) -> set[str]:
    if not cursor:
        return set()
    try:
        payload = json.loads(cursor)
    except json.JSONDecodeError:
        return set()
    seen = payload.get("seen_experiment_folder_ids", [])
    return {str(folder_id) for folder_id in seen}


def _serialize_seen_folder_ids(seen: set[str]) -> str:
    return json.dumps({"seen_experiment_folder_ids": sorted(seen)})


def _parse_last_calibrant_file_id(cursor: str | None) -> str | None:
    if not cursor:
        return None
    try:
        payload = json.loads(cursor)
    except json.JSONDecodeError:
        return None

    calibrant_file_id = payload.get("last_calibrant_file_id")
    if not calibrant_file_id:
        return None
    return str(calibrant_file_id)


def _serialize_last_calibrant_file_id(file_id: str) -> str:
    return json.dumps({"last_calibrant_file_id": file_id})


def _list_candidate_experiments_legacy(gc: Any, root_folder_id: str, calibrants_folder_id: str | None) -> list[dict[str, Any]]:
    candidate_experiments: list[dict[str, Any]] = []
    for folder in _get_child_folders(gc, root_folder_id):
        folder_id = str(folder.get("_id", ""))
        if not folder_id or folder_id == calibrants_folder_id:
            continue

        raw_folder = _find_child_folder_by_name(gc, folder_id, "raw")
        if not raw_folder:
            continue

        if _raw_folder_has_scan_files(gc, str(raw_folder["_id"])):
            candidate_experiments.append(folder)
    return candidate_experiments


def _list_candidate_experiments_datafiles(gc: Any, context: SensorEvaluationContext) -> list[dict[str, Any]]:
    rows = call_with_retries(list_experiment_candidates_from_datafiles, gc)
    return [
        {
            "_id": row.experiment_folder_id,
            "name": row.experiment_folder_name,
            "raw_folder_id": row.raw_folder_id,
        }
        for row in rows
    ]


def _latest_calibrant_scan_file_id_datafiles(gc: Any) -> str | None:
    candidate = call_with_retries(latest_calibrant_candidate_from_datafiles, gc)
    if not candidate:
        return None
    return candidate.file_id


@sensor(
    name="calibration_scan_sensor",
    job_name="calibration_precompute",
    minimum_interval_seconds=30,
    required_resource_keys={"GirderClient"},
)
def calibration_scan_sensor(context: SensorEvaluationContext, GirderClient=None):
    calibrants_folder_id = os.getenv("GIRDER_CALIBRANTS_FOLDER_ID")
    if not calibrants_folder_id:
        return SkipReason("GIRDER_CALIBRANTS_FOLDER_ID is not configured.")

    gc = GirderClient or context.resources.GirderClient
    backend = get_discovery_backend()

    try:
        if backend == DISCOVERY_BACKEND_DATAFILES:
            latest_file_id = _latest_calibrant_scan_file_id_datafiles(gc)
        else:
            latest_file_id = _latest_calibrant_scan_file_id(gc, calibrants_folder_id)
    except Exception as exc:
        if backend == DISCOVERY_BACKEND_DATAFILES and should_allow_discovery_fallback():
            context.log.warning(
                "Datafiles calibrant discovery failed (%s). Falling back to legacy discovery.",
                exc,
            )
            latest_file_id = _latest_calibrant_scan_file_id(gc, calibrants_folder_id)
        else:
            raise

    if not latest_file_id:
        return SkipReason("No calibrant scan files were detected.")

    previous_file_id = _parse_last_calibrant_file_id(context.cursor)
    if previous_file_id == latest_file_id:
        return SkipReason("No new calibrant scan files detected.")

    return SensorResult(
        cursor=_serialize_last_calibrant_file_id(latest_file_id),
        run_requests=[
            RunRequest(
                run_key=f"calibrant:{latest_file_id}",
                job_name="calibration_precompute",
                tags={"calibrant_scan_file_id": latest_file_id},
            )
        ],
    )


@sensor(
    name="experiment_folder_sensor",
    job_name="xrd",
    minimum_interval_seconds=30,
    required_resource_keys={"GirderClient"},
)
def experiment_folder_sensor(context: SensorEvaluationContext, GirderClient=None):
    root_folder_id = os.getenv("GIRDER_ROOT_FOLDER_ID")
    calibrants_folder_id = os.getenv("GIRDER_CALIBRANTS_FOLDER_ID")

    if not root_folder_id:
        return SkipReason("GIRDER_ROOT_FOLDER_ID is not configured.")

    gc = GirderClient or context.resources.GirderClient

    previously_seen = _parse_seen_folder_ids(context.cursor)
    backend = get_discovery_backend()

    try:
        if backend == DISCOVERY_BACKEND_DATAFILES:
            candidate_experiments = _list_candidate_experiments_datafiles(gc, context)
            if calibrants_folder_id:
                candidate_experiments = [
                    exp
                    for exp in candidate_experiments
                    if str(exp.get("_id", "")) != calibrants_folder_id
                ]
        else:
            candidate_experiments = _list_candidate_experiments_legacy(gc, root_folder_id, calibrants_folder_id)
    except Exception as exc:
        if backend == DISCOVERY_BACKEND_DATAFILES and should_allow_discovery_fallback():
            context.log.warning(
                "Datafiles experiment discovery failed (%s). Falling back to legacy discovery.",
                exc,
            )
            candidate_experiments = _list_candidate_experiments_legacy(
                gc,
                root_folder_id,
                calibrants_folder_id,
            )
        else:
            raise

    if not candidate_experiments:
        return SkipReason("No experiments with raw scan files detected.")

    new_experiments = [exp for exp in candidate_experiments if str(exp["_id"]) not in previously_seen]
    if not new_experiments:
        return SkipReason("No new experiment folders detected.")

    run_requests = [
        RunRequest(
            run_key=f"experiment:{exp['_id']}",
            job_name="xrd",
            partition_key=str(exp["_id"]),
            tags={"experiment_folder_name": str(exp.get("name", "unknown"))},
        )
        for exp in new_experiments
    ]

    all_seen = previously_seen | {str(exp["_id"]) for exp in new_experiments}

    return SensorResult(
        cursor=_serialize_seen_folder_ids(all_seen),
        run_requests=run_requests,
        dynamic_partitions_requests=[
            experiment_partitions.build_add_request([str(exp["_id"]) for exp in new_experiments])
        ],
    )
