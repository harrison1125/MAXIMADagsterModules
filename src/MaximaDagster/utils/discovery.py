import os
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExperimentCandidate:
    experiment_folder_id: str
    experiment_folder_name: str
    raw_folder_id: str | None
    created: str = ""
    file_id: str = ""


@dataclass(frozen=True)
class CalibrantCandidate:
    file_id: str
    item_id: str | None
    file_name: str
    created: str


@dataclass(frozen=True)
class XrdRawScanCandidate:
    file_id: str
    item_id: str
    file_name: str
    created: str
    raw_folder_id: str
    experiment_folder_id: str
    igsn: str | None


class DatafilesDiscoveryError(RuntimeError):
    pass


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


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text

def get_model_type(row: dict[str, Any]) -> str | None:
    model_type = row.get("_modelType")
    return _as_str(model_type)


def get_item_id(row: dict[str, Any]) -> str | None:
    if get_model_type(row) == "item":
        id = row.get("_id")
    else:
        id = None
    return _as_str(id)

def get_fname(row: dict[str, Any]) -> str | None:
    value = row.get("name")
    return _as_str(value)


def get_meta(row: dict[str, Any]) -> dict[str, Any]:
    meta = row.get("meta")
    if isinstance(meta, dict):
        return meta
    return {}

def get_experiment_date(row: dict[str, Any]) -> str | None:
    meta = get_meta(row)
    value = meta.get("experiment_date")
    return _as_str(value)


def get_folder_id(row: dict[str, Any]) -> str | None:
    value = row.get("folderId")
    return _as_str(value)

def get_igsn(row: dict[str, Any]) -> str | None:
    meta = get_meta(row)
    value = meta.get("igsn")
    return _as_str(value)


def fetch_partitions(gc: Any, data_type: str, since: str) -> dict[str, str]:
    response = gc.get(
        "aimdl/partition",
        parameters={
            "dataType": data_type,
            "since": since,
        },
    )
    if response is None:
        return {}
    if not isinstance(response, dict):
        raise DatafilesDiscoveryError("Expected dict response from aimdl/partition")

    normalized: dict[str, str] = {}
    for key, checksum in response.items():
        partition_key = _as_str(key)
        checksum_text = _as_str(checksum)
        if not partition_key or not checksum_text:
            continue
        normalized[partition_key] = checksum_text
    return normalized


def fetch_partition_details(gc: Any, key: str, data_type: str) -> list[dict[str, Any]]:
    response = gc.get(
        "aimdl/partition/details",
        parameters={
            "key": key,
            "dataType": data_type,
        },
    )
    if response is None:
        return []
    if not isinstance(response, list):
        raise DatafilesDiscoveryError("Expected list response from aimdl/partition/details")

    rows: list[dict[str, Any]] = []
    for row in response:
        if isinstance(row, dict):
            rows.append(row)
    return rows


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
