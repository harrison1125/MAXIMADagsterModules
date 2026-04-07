import os
import time
from dataclasses import dataclass
from typing import Any


DISCOVERY_BACKEND_LEGACY = "legacy"
DISCOVERY_BACKEND_DATAFILES = "datafiles"


@dataclass(frozen=True)
class ExperimentCandidate:
    experiment_folder_id: str
    experiment_folder_name: str
    raw_folder_id: str | None


@dataclass(frozen=True)
class CalibrantCandidate:
    file_id: str
    item_id: str | None
    file_name: str
    created: str


class DatafilesDiscoveryError(RuntimeError):
    pass


def get_discovery_backend() -> str:
    backend = str(os.getenv("DISCOVERY_BACKEND", DISCOVERY_BACKEND_LEGACY)).strip().lower()
    if backend in {DISCOVERY_BACKEND_LEGACY, DISCOVERY_BACKEND_DATAFILES}:
        return backend
    return DISCOVERY_BACKEND_LEGACY


def should_allow_discovery_fallback() -> bool:
    value = str(os.getenv("DISCOVERY_ALLOW_FALLBACK", "true")).strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_datafiles_limit() -> int:
    raw = str(os.getenv("DISCOVERY_DATAFILES_LIMIT", "200")).strip()
    try:
        value = int(raw)
    except ValueError:
        return 200
    if value <= 0:
        return 200
    return min(value, 1000)


def get_datafiles_retry_count() -> int:
    raw = str(os.getenv("DISCOVERY_DATAFILES_RETRY_COUNT", "2")).strip()
    try:
        value = int(raw)
    except ValueError:
        return 2
    return max(0, min(value, 5))


def get_datafiles_retry_delay_seconds() -> float:
    raw = str(os.getenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0.5")).strip()
    try:
        value = float(raw)
    except ValueError:
        return 0.5
    return max(0.0, min(value, 10.0))


def _pick_first(mapping: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _file_object(row: dict[str, Any]) -> dict[str, Any]:
    nested = row.get("file")
    if isinstance(nested, dict):
        return nested
    return row


def _folder_object(row: dict[str, Any]) -> dict[str, Any]:
    nested = row.get("folder")
    if isinstance(nested, dict):
        return nested
    return row


def _item_object(row: dict[str, Any]) -> dict[str, Any]:
    nested = row.get("item")
    if isinstance(nested, dict):
        return nested
    return row


def _extract_file_id(row: dict[str, Any]) -> str | None:
    value = _pick_first(row, ["_id", "id", "itemId", "item_id"])
    return _as_str(value)


def _extract_file_name(row: dict[str, Any]) -> str | None:
    value = _pick_first(row, ["name", "fileName", "file_name"])
    return _as_str(value)


def _extract_created(row: dict[str, Any]) -> str:
    value = _pick_first(row, ["created", "createdAt", "created_at"])
    text = _as_str(value)
    if text:
        return text
    return ""


def _extract_item_id(row: dict[str, Any]) -> str | None:
    value = _pick_first(row, ["_id", "id", "itemId", "item_id"])
    return _as_str(value)


def _extract_experiment_folder_id(row: dict[str, Any]) -> str | None:
    value = _pick_first(
        row,
        [
            "experimentFolderId",
            "experiment_folder_id",
            "experimentId",
            "experiment_id",
        ],
    )
    return _as_str(value)


def _extract_raw_folder_id(row: dict[str, Any]) -> str | None:
    value = _pick_first(row, ["folderId", "folder_id", "parentId", "parent_id"])
    return _as_str(value)


def _extract_experiment_name(row: dict[str, Any]) -> str | None:
    value = _pick_first(row, ["experimentFolderName", "experiment_folder_name", "experimentName"])
    return _as_str(value)


def _get_folder_by_id_cached(gc: Any, folder_id: str, cache: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if folder_id in cache:
        return cache[folder_id]
    try:
        folder_info = gc.getFolder(folder_id)
        cache[folder_id] = folder_info
        return folder_info
    except Exception:
        cache[folder_id] = {}
        return None


def _fetch_datafiles_page(gc: Any, data_type: str, limit: int, offset: int) -> list[dict[str, Any]]:
    rows = gc.get(
        "aimdl/datafiles",
        parameters={
            "dataType": data_type,
            "limit": limit,
            "offset": offset,
            "sort": "updated",
            "sortdir": -1,
        },
    )
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise DatafilesDiscoveryError("Expected list response from aimdl/datafiles")
    return rows


def _iter_datafiles_rows(gc: Any, data_type: str, limit: int) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = _fetch_datafiles_page(gc, data_type=data_type, limit=limit, offset=offset)
        if not page:
            break
        all_rows.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return all_rows


def _sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (_extract_created(row), _extract_file_id(row) or ""),
        reverse=True,
    )


def list_experiment_candidates_from_datafiles(gc: Any) -> list[ExperimentCandidate]:
    rows = _iter_datafiles_rows(gc, data_type="xrd_raw", limit=get_datafiles_limit())
    rows = _sort_rows(rows)

    folder_cache: dict[str, dict[str, Any]] = {}
    by_experiment: dict[str, ExperimentCandidate] = {}

    for row in rows:
        raw_folder_id = _extract_raw_folder_id(row)
        file_id = _extract_file_id(row)
        if not raw_folder_id or not file_id:
            continue

        experiment_folder_id = _extract_experiment_folder_id(row)
        if not experiment_folder_id:
            parent_folder = _get_folder_by_id_cached(gc, raw_folder_id, folder_cache)
            if parent_folder:
                experiment_folder_id = parent_folder.get("parentId")
                if experiment_folder_id:
                    experiment_folder_id = str(experiment_folder_id)

        if not experiment_folder_id:
            experiment_folder_id = raw_folder_id

        experiment_folder_name = _extract_experiment_name(row)
        if not experiment_folder_name and experiment_folder_id != raw_folder_id:
            exp_folder = _get_folder_by_id_cached(gc, experiment_folder_id, folder_cache)
            if exp_folder:
                experiment_folder_name = exp_folder.get("name")
        if not experiment_folder_name:
            experiment_folder_name = "unknown"

        by_experiment.setdefault(
            experiment_folder_id,
            ExperimentCandidate(
                experiment_folder_id=experiment_folder_id,
                experiment_folder_name=str(experiment_folder_name),
                raw_folder_id=raw_folder_id,
            ),
        )

    return list(by_experiment.values())


def latest_calibrant_candidate_from_datafiles(gc: Any) -> CalibrantCandidate | None:
    rows = _iter_datafiles_rows(gc, data_type="xrd_calibrant_raw", limit=get_datafiles_limit())
    if not rows:
        return None

    for row in _sort_rows(rows):
        file_id = _extract_file_id(row)
        file_name = _extract_file_name(row)
        if not file_id or not file_name:
            continue
        return CalibrantCandidate(
            file_id=file_id,
            item_id=_extract_item_id(row),
            file_name=file_name,
            created=_extract_created(row),
        )

    return None


def call_with_retries(fn, *args, **kwargs):
    retry_count = get_datafiles_retry_count()
    retry_delay_seconds = get_datafiles_retry_delay_seconds()

    last_error: Exception | None = None
    for attempt in range(retry_count + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc: 
            last_error = exc
            if attempt >= retry_count:
                break
            if retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)

    if last_error is None:
        raise DatafilesDiscoveryError("Datafiles call failed without captured exception")
    raise last_error
