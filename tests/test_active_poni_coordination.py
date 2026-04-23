from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import RetryRequested, build_asset_context

from MaximaDagster import assets


class _NoopGirderClient:
    pass


def test_active_poni_retries_on_cache_miss(monkeypatch) -> None:
    monkeypatch.setattr(assets, "_get_target_calibrant_file_id", lambda gc, partition_key: "cal_1")
    monkeypatch.setattr(assets.CalibrationCache, "get_entry_for_calibrant", lambda self, *args, **kwargs: None)

    context = build_asset_context(
        resources={"GirderClient": _NoopGirderClient()},
        partition_key="IGSN-1//2026-01-01T00:00:00+00:00",
    )

    with pytest.raises(RetryRequested) as exc_info:
        assets.active_poni(
            context,
            calibration_model={"metadata": {"version": "1.0", "source_file_id": "model_1"}},
        )

    assert exc_info.value.max_retries > 0
    assert exc_info.value.seconds_to_wait > 0


def test_active_poni_returns_cached_payload_on_cache_hit(monkeypatch) -> None:
    cache_entry = SimpleNamespace(
        poni_path=Path("data/calibrations/test.poni"),
        poni_item_id="poni_item_1",
        calibrant_scan_file_id="cal_9",
    )
    geometry = SimpleNamespace(dist=1.0)

    monkeypatch.setattr(assets, "_get_target_calibrant_file_id", lambda gc, partition_key: "cal_9")
    monkeypatch.setattr(assets.CalibrationCache, "get_entry_for_calibrant", lambda self, *args, **kwargs: cache_entry)
    monkeypatch.setattr(assets, "load_geometry_from_poni", lambda path: geometry)

    context = build_asset_context(
        resources={"GirderClient": _NoopGirderClient()},
        partition_key="IGSN-1//2026-01-01T00:00:00+00:00",
    )

    result = assets.active_poni(
        context,
        calibration_model={"metadata": {"version": "1.0", "source_file_id": "model_1"}},
    )

    assert set(result.keys()) == {
        "geometry",
        "poni_path",
        "poni_item_id",
        "calibrant_scan_file_id",
        "cache_hit",
    }
    assert result["geometry"] is geometry
    assert Path(result["poni_path"]) == Path("data/calibrations/test.poni")
    assert result["poni_item_id"] == "poni_item_1"
    assert result["calibrant_scan_file_id"] == "cal_9"
    assert result["cache_hit"] is True