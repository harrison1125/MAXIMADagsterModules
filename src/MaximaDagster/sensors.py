import json
import os
import tempfile
from typing import Any

import h5py
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
    latest = _latest_calibrant_scan_legacy(gc, calibrants_folder_id)
    return str(latest["file_id"]) if latest else None


def _read_xrd_h5_from_file_id(gc: Any, file_id: str) -> Any:
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, f"{file_id}.h5")
        gc.downloadFile(file_id, local_path)
        with h5py.File(local_path, "r") as h5f:
            return h5f["entry/data/data"][:][0]


def _parse_cursor_payload(cursor: str | None) -> dict[str, Any]:
    if not cursor:
        return {}
    try:
        payload = json.loads(cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _is_newer_created_file_id(
    created: str,
    file_id: str,
    cursor_created: str,
    cursor_file_id: str,
) -> bool:
    if not cursor_created and not cursor_file_id:
        return True
    if not cursor_created:
        return str(file_id) != str(cursor_file_id)
    candidate_key = (str(created), str(file_id))
    cursor_key = (str(cursor_created), str(cursor_file_id))
    return candidate_key > cursor_key


def _serialize_experiment_cursor(
    seen_experiment_folder_ids: set[str],
    last_created: str,
    last_file_id: str,
) -> str:
    return json.dumps(
        {
            "seen_experiment_folder_ids": sorted(seen_experiment_folder_ids),
            "last_created": last_created,
            "last_file_id": last_file_id,
        }
    )


def _parse_experiment_cursor(cursor: str | None) -> tuple[set[str], str, str]:
    payload = _parse_cursor_payload(cursor)
    seen = payload.get("seen_experiment_folder_ids", [])
    seen_experiment_folder_ids = {str(folder_id) for folder_id in seen}
    last_created = str(payload.get("last_created") or "")
    last_file_id = str(payload.get("last_file_id") or "")
    return seen_experiment_folder_ids, last_created, last_file_id


def _serialize_calibrant_cursor(last_created: str, last_file_id: str) -> str:
    return json.dumps(
        {
            "last_created": last_created,
            "last_file_id": last_file_id,
        }
    )


def _parse_calibrant_cursor(cursor: str | None) -> tuple[str, str]:
    payload = _parse_cursor_payload(cursor)
    last_created = str(payload.get("last_created") or "")
    last_file_id = str(payload.get("last_file_id") or payload.get("last_calibrant_file_id") or "")
    return last_created, last_file_id


def _latest_calibrant_scan_legacy(gc: Any, calibrants_folder_id: str, include_xrd: bool = False) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []

    for item in gc.listItem(calibrants_folder_id):
        item_created = str(item.get("created") or item.get("updated") or "")
        for file_obj in gc.listFile(item["_id"]):
            file_name = str(file_obj.get("name", ""))
            if not _CALIBRANT_SCAN_PATTERN.match(file_name):
                continue

            file_id = str(file_obj.get("_id", ""))
            if not file_id:
                continue

            created = str(file_obj.get("created") or file_obj.get("updated") or item_created)
            candidates.append(
                {
                    "file_id": file_id,
                    "item_id": str(item["_id"]),
                    "file_name": file_name,
                    "created": created,
                    "updated": created,
                }
            )

    if not candidates:
        return None

    latest = max(candidates, key=lambda c: (c["created"], c["file_id"]))
    if include_xrd:
        latest["xrd"] = _read_xrd_h5_from_file_id(gc, latest["file_id"])
    return latest


def _latest_calibrant_scan_candidate(gc: Any) -> dict[str, Any] | None:
    backend = get_discovery_backend()

    if backend == DISCOVERY_BACKEND_DATAFILES:
        try:
            candidate = call_with_retries(latest_calibrant_candidate_from_datafiles, gc)
            if candidate is None:
                return None
            return {
                "file_id": candidate.file_id,
                "item_id": candidate.item_id,
                "file_name": candidate.file_name,
                "created": candidate.created,
            }
        except Exception:
            if not should_allow_discovery_fallback():
                raise
            calibrants_folder_id = os.getenv("GIRDER_CALIBRANTS_FOLDER_ID")
            if not calibrants_folder_id:
                raise ValueError(
                    "GIRDER_CALIBRANTS_FOLDER_ID is required when discovery fallback is enabled."
                )
            return _latest_calibrant_scan_legacy(gc, calibrants_folder_id)

    calibrants_folder_id = os.getenv("GIRDER_CALIBRANTS_FOLDER_ID")
    if not calibrants_folder_id:
        raise ValueError("GIRDER_CALIBRANTS_FOLDER_ID is not configured.")
    return _latest_calibrant_scan_legacy(gc, calibrants_folder_id)


def _parse_seen_folder_ids(cursor: str | None) -> set[str]:
    payload = _parse_cursor_payload(cursor)
    seen = payload.get("seen_experiment_folder_ids", [])
    return {str(folder_id) for folder_id in seen}


def _serialize_seen_folder_ids(seen: set[str]) -> str:
    return _serialize_experiment_cursor(seen, "", "")


def _parse_last_calibrant_file_id(cursor: str | None) -> str | None:
    _, last_file_id = _parse_calibrant_cursor(cursor)
    return last_file_id or None


def _serialize_last_calibrant_file_id(file_id: str) -> str:
    return _serialize_calibrant_cursor("", file_id)


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
            "created": row.created,
            "file_id": row.file_id,
        }
        for row in rows
    ]


def _latest_calibrant_scan_file_id_datafiles(gc: Any) -> str | None:
    candidate = _latest_calibrant_scan_candidate(gc)
    if not candidate:
        return None
    return str(candidate["file_id"])


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
    last_created, last_file_id = _parse_calibrant_cursor(context.cursor)

    try:
        candidate = _latest_calibrant_scan_candidate(gc)
    except Exception as exc:
        if backend == DISCOVERY_BACKEND_DATAFILES and should_allow_discovery_fallback():
            context.log.warning(
                "Datafiles calibrant discovery failed (%s). Falling back to legacy discovery.",
                exc,
            )
            candidate = _latest_calibrant_scan_legacy(gc, calibrants_folder_id)
        else:
            raise

    if not candidate:
        return SkipReason("No calibrant scan files were detected.")

    if not context.cursor:
        return SensorResult(
            cursor=_serialize_calibrant_cursor(str(candidate["created"]), str(candidate["file_id"])),
            run_requests=[],
        )

    if not _is_newer_created_file_id(
        str(candidate["created"]),
        str(candidate["file_id"]),
        last_created,
        last_file_id,
    ):
        return SkipReason("No new calibrant scan files detected.")

    return SensorResult(
        cursor=_serialize_calibrant_cursor(str(candidate["created"]), str(candidate["file_id"])),
        run_requests=[
            RunRequest(
                run_key=f"calibrant:{candidate['file_id']}",
                job_name="calibration_precompute",
                tags={"calibrant_scan_file_id": str(candidate["file_id"])},
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

    previously_seen, last_created, last_file_id = _parse_experiment_cursor(context.cursor)
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

    if not context.cursor:
        newest = max(candidate_experiments, key=lambda exp: (str(exp.get("created") or ""), str(exp.get("file_id") or "")))
        return SensorResult(
            cursor=_serialize_experiment_cursor(
                {str(exp["_id"]) for exp in candidate_experiments},
                str(newest.get("created") or ""),
                str(newest.get("file_id") or ""),
            ),
            run_requests=[],
            dynamic_partitions_requests=[],
        )

    new_experiments = [
        exp
        for exp in candidate_experiments
        if str(exp["_id"]) not in previously_seen
        and (
            not backend == DISCOVERY_BACKEND_DATAFILES
            or _is_newer_created_file_id(
                str(exp.get("created") or ""),
                str(exp.get("file_id") or ""),
                last_created,
                last_file_id,
            )
        )
    ]
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
    newest = max(new_experiments, key=lambda exp: (str(exp.get("created") or ""), str(exp.get("file_id") or "")))

    return SensorResult(
        cursor=_serialize_experiment_cursor(
            all_seen,
            str(newest.get("created") or ""),
            str(newest.get("file_id") or ""),
        ),
        run_requests=run_requests,
        dynamic_partitions_requests=[
            experiment_partitions.build_add_request([str(exp["_id"]) for exp in new_experiments])
        ],
    )
