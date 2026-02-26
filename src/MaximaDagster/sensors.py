import json
import os
import re
from typing import Any

from dagster import (
    DynamicPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)


experiment_partitions = DynamicPartitionsDefinition(name="experiments")
_H5_SCAN_PATTERN = re.compile(r"^scan_point_\d+_data_\d+\.h5$", re.IGNORECASE)


def _get_child_folders(gc: Any, parent_folder_id: str) -> list[dict[str, Any]]:
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


def _find_child_folder_by_name(gc: Any, parent_folder_id: str, name: str) -> dict[str, Any] | None:
    for folder in _get_child_folders(gc, parent_folder_id):
        if folder.get("name", "").lower() == name.lower():
            return folder
    return None


def _raw_folder_has_scan_files(gc: Any, raw_folder_id: str) -> bool:
    for item in gc.listItem(raw_folder_id):
        for file_obj in gc.listFile(item["_id"]):
            if _H5_SCAN_PATTERN.match(file_obj.get("name", "")):
                return True
    return False


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


@sensor(
    name="experiment_folder_sensor",
    job_name="xrd_test_job",
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

    if not candidate_experiments:
        return SkipReason("No experiments with raw scan files detected.")

    new_experiments = [exp for exp in candidate_experiments if str(exp["_id"]) not in previously_seen]
    if not new_experiments:
        return SkipReason("No new experiment folders detected.")

    run_requests = [
        RunRequest(
            run_key=f"experiment:{exp['_id']}",
            job_name="xrd_test_job",
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
