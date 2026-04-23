from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import h5py
import numpy as np
import pandas as pd
import pytest
from dagster import build_asset_context

from MaximaDagster import assets


def _write_test_h5(path: Path) -> None:
    with h5py.File(path, "w") as h5f:
        h5f.create_dataset("entry/data/data", data=np.arange(4, dtype=np.float32).reshape(1, 2, 2))


class _XrdRawClient:
    def __init__(self, h5_path: Path, detail_rows):
        self._h5_path = h5_path
        self._detail_rows = detail_rows

    def get(self, route, parameters=None):
        if route == "aimdl/partition/details":
            return list(self._detail_rows)
        raise AssertionError(route)

    def listFile(self, item_id):
        return [{"_id": f"file_{item_id}", "name": "scan_point_0_data_00001.h5"}]

    def downloadFile(self, file_id, local_path):
        _ = file_id
        Path(local_path).write_bytes(self._h5_path.read_bytes())

    def getFolder(self, folder_id):
        if folder_id == "raw_01":
            return {
                "_id": "raw_01",
                "name": "raw",
                "parentCollection": "folder",
                "parentId": "exp_01",
            }
        if folder_id == "exp_01":
            return {"_id": "exp_01", "name": "experiment_01"}
        raise AssertionError(folder_id)


class _LogStub:
    def info(self, msg):
        _ = msg

    def warning(self, msg):
        _ = msg


class _AssetContextStub:
    def __init__(self, resources, partition_key, run_id):
        self.resources = SimpleNamespace(**resources)
        self.partition_key = partition_key
        self.run = SimpleNamespace(run_id=run_id)
        self.log = _LogStub()


def test_xrd_raw_preserves_igsn_and_source_shape(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    h5_path = tmp_path / "scan.h5"
    _write_test_h5(h5_path)

    rows = [
        {
            "_modelType": "item",
            "_id": "item_1",
            "name": "scan_point_0_data_00001.h5",
            "folderId": "raw_01",
            "meta": {"igsn": "IGSN-123"},
        }
    ]
    gc = _XrdRawClient(h5_path=h5_path, detail_rows=rows)

    context = build_asset_context(resources={"GirderClient": gc}, partition_key="exp_01")
    result = assets.xrd_raw(context)

    assert result["experiment_folder_id"] == "exp_01"
    assert result["experiment_name"] == "experiment_01"
    assert result["folder_id"] == "raw_01"
    assert set(result["scans"][0].keys()) == {"igsn", "xrd", "source_files", "source_item_ids"}
    assert result["scans"][0]["igsn"] == "IGSN-123"
    assert result["scans"][0]["source_files"] == ["scan_point_0_data_00001.h5"]
    assert result["scans"][0]["source_item_ids"] == ["item_1"]


# def test_xrd_raw_conflicting_igsn_for_same_scan_fails_fast(tmp_path, monkeypatch) -> None:
#     monkeypatch.chdir(tmp_path)
#     h5_path = tmp_path / "scan.h5"
#     _write_test_h5(h5_path)

#     rows = [
#         {
#             "_modelType": "item",
#             "_id": "item_1",
#             "name": "scan_point_0_data_00001.h5",
#             "folderId": "raw_01",
#             "meta": {"igsn": "IGSN-A"},
#         },
#         {
#             "_modelType": "item",
#             "_id": "item_2",
#             "name": "scan_point_0_data_00001.h5",
#             "folderId": "raw_01",
#             "meta": {"igsn": "IGSN-B"},
#         },
#     ]
#     gc = _XrdRawClient(h5_path=h5_path, detail_rows=rows)

#     context = build_asset_context(resources={"GirderClient": gc}, partition_key="exp_01")

#     with pytest.raises(ValueError, match="Conflicting IGSN"):
#         assets.xrd_raw(context)


def test_azimuthal_integration_uploads_exact_metadata_shape(monkeypatch) -> None:
    monkeypatch.setenv("GIRDER_API_URL", "https://girder.example/api/v1")

    uploaded = []

    def _fake_upload_artifact(**kwargs):
        uploaded.append(kwargs)
        return f"item_{len(uploaded)}"

    monkeypatch.setattr(assets, "upload_artifact", _fake_upload_artifact)
    monkeypatch.setattr(
        assets.AzimuthalIntegrator,
        "integrate_dict",
        lambda xrd_scans, geometry: {
            0: pd.DataFrame({"q_nm^-1": [1.0], "intensity": [2.0]}),
            1: pd.DataFrame({"q_nm^-1": [3.0], "intensity": [4.0]}),
        },
    )

    context = _AssetContextStub(
        resources={"GirderClient": object()},
        partition_key="exp_01",
        run_id="run_123",
    )

    xrd_payload = {
        "experiment_folder_id": "exp_01",
        "experiment_name": "experiment_01",
        "scans": {
            0: {"igsn": "IGSN-123", "xrd": np.array([1, 2, 3])},
            1: {"xrd": np.array([4, 5, 6])},
        },
    }
    active_poni_payload = {
        "poni_item_id": "poni_item_1",
        "geometry": SimpleNamespace(dist=1.1, poni1=2.2, poni2=3.3, rot1=4.4, rot2=5.5, rot3=6.6),
    }

    result = assets.azimuthal_integration.op.compute_fn.decorated_fn(
        context,
        xrd_raw=xrd_payload,
        active_poni=active_poni_payload,
    )

    assert set(result.keys()) == {0, 1}
    assert len(uploaded) == 2

    first = uploaded[0]["metadata"]
    second = uploaded[1]["metadata"]

    assert set(first.keys()) == {"prov", "poni", "dataType", "igsn"}
    assert set(second.keys()) == {"prov", "poni", "dataType"}

    assert set(first["prov"].keys()) == {"workflow_version", "run_id", "time"}
    assert first["prov"]["run_id"] == "run_123"
    assert first["dataType"] == "xrd_derived"
    assert first["igsn"] == "IGSN-123"

    assert set(first["poni"].keys()) == {"item_id", "link", "geometry"}
    assert first["poni"]["item_id"] == "poni_item_1"
    assert first["poni"]["link"] == "https://girder.example/#item/poni_item_1"
    assert set(first["poni"]["geometry"].keys()) == {"dist", "poni1", "poni2", "rot1", "rot2", "rot3"}


def test_poni_uploads_exact_metadata_shape(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GIRDER_API_URL", "https://girder.example/api/v1")

    captured = {}

    class _FakeCalibrator:
        def __init__(self, model_path, calibrant, detector, energy):
            _ = (model_path, calibrant, detector, energy)

        def calibrate(self, pattern, output_path):
            _ = pattern
            Path(output_path).write_text("poni", encoding="utf-8")
            return SimpleNamespace(dist=1.0, poni1=2.0, poni2=3.0, rot1=4.0, rot2=5.0, rot3=6.0)

    def _fake_upload_artifact(**kwargs):
        captured.update(kwargs)
        return "poni_item_1"

    monkeypatch.setattr(assets.calibrate, "MaximaCalibrator", _FakeCalibrator)
    monkeypatch.setattr(assets, "upload_artifact", _fake_upload_artifact)

    context = _AssetContextStub(
        resources={"GirderClient": object()},
        partition_key="cal_01",
        run_id="run_poni",
    )

    result = assets.poni.op.compute_fn.decorated_fn(
        context,
        calibration_model={
            "model_path": "model.pth",
            "metadata": {
                "calibrant": "alpha_Al2O3",
                "detector": "Eiger2Cdte_1M",
                "energy": 12.0,
                "version": "1.2.3",
                "source_file_id": "model_file_1",
            },
        },
        xrd_calibrant_raw={
            "calibrant_item_id": "cal_item_1",
            "calibrant_file_name": "xrd_calibrant_data_000001.h5",
            "igsn": "CAL-IGSN-1",
            "pattern": np.array([1.0, 2.0]),
            "folder_id": "cal_folder",
        },
    )

    metadata = captured["metadata"]
    assert set(metadata.keys()) == {"prov", "model", "calibrant", "dataType"}
    assert set(metadata["prov"].keys()) == {"workflow_version", "run_id", "time"}
    assert metadata["prov"]["run_id"] == "run_poni"
    assert set(metadata["model"].keys()) == {"version", "item_id", "link"}
    assert metadata["model"] == {
        "version": "1.2.3",
        "item_id": "model_file_1",
        "link": "https://girder.example/#item/model_file_1",
    }
    assert set(metadata["calibrant"].keys()) == {"item_id", "link", "igsn"}
    assert metadata["calibrant"] == {
        "item_id": "cal_item_1",
        "link": "https://girder.example/#item/cal_item_1",
        "igsn": "CAL-IGSN-1",
    }
    assert metadata["dataType"] == "xrd_calibrant_derived"

    assert set(result.keys()) == {"poni_path", "poni_item_id"}
    assert result["poni_item_id"] == "poni_item_1"