from pathlib import Path

import h5py
import numpy as np
import pandas as pd
from dagster import build_asset_context

from MaximaDagster import assets


def _write_test_h5(path: Path) -> None:
	with h5py.File(path, "w") as h5f:
		h5f.create_dataset("entry/data/data", data=np.arange(4, dtype=np.float32).reshape(1, 2, 2))


class _FakeXrdGirderClient:
	def __init__(self, h5_path: Path, xrf_path: Path):
		self._sources = {
			"file_h5": h5_path,
			"file_xrf": xrf_path,
		}

	def getFolder(self, folder_id):
		return {"_id": folder_id, "name": "exp_name"}

	def get(self, route, parameters=None):
		assert route == "folder"
		if (parameters or {}).get("parentId") == "exp_01":
			return [{"_id": "raw_01", "name": "raw"}]
		return []

	def listItem(self, folder_id):
		if folder_id == "raw_01":
			return [{"_id": "item_1", "meta": {"igsn": "IGSN-123"}}]
		return []

	def listFile(self, item_id):
		if item_id == "item_1":
			return [
				{"_id": "file_h5", "name": "scan_point_0_data_00001.h5"},
				{"_id": "file_xrf", "name": "scan_point_0.xrf"},
			]
		return []

	def downloadFile(self, file_id, local_path):
		Path(local_path).write_bytes(self._sources[file_id].read_bytes())


class _FakePublishGirderClient:
	def __init__(self):
		self._items = {}
		self.metadata_calls = []

	def loadOrCreateItem(self, filename, folder_id):
		item = self._items.get(filename)
		if item is None:
			item = {"_id": f"item_{len(self._items) + 1}", "name": filename, "folder_id": folder_id}
			self._items[filename] = item
		return item

	def get(self, route, parameters=None):
		if route.startswith("item/") and route.endswith("/files"):
			return []
		raise AssertionError(route)

	def post(self, route, parameters=None):
		assert route == "file"
		return {"_id": f"file_meta_{len(self._items)}", **(parameters or {})}

	def _uploadContents(self, file_meta, stream, size):
		_ = stream
		return {"_id": f"uploaded_{file_meta['_id']}", "size": size}

	def uploadFileContents(self, file_id, stream, size):
		_ = stream
		return {"_id": file_id, "size": size}

	def addMetadataToItem(self, item_id, metadata):
		self.metadata_calls.append((item_id, metadata))


def test_xrdxrf_scans_passes_igsn_through(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)

	h5_path = tmp_path / "source.h5"
	xrf_path = tmp_path / "source.xrf"
	_write_test_h5(h5_path)
	xrf_path.write_text("xrf-data", encoding="utf-8")

	gc = _FakeXrdGirderClient(h5_path=h5_path, xrf_path=xrf_path)
	context = build_asset_context(resources={"GirderClient": gc}, partition_key="exp_01")

	result = assets.xrdxrf_scans(context)

	assert result["scans"][0]["igsn"] == "IGSN-123"


def test_publish_xrd_results_adds_igsn_metadata_to_csv_items(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)

	poni_path = tmp_path / "calibration.poni"
	poni_path.write_text("poni", encoding="utf-8")

	gc = _FakePublishGirderClient()
	context = build_asset_context(resources={"GirderClient": gc}, partition_key="exp_01")

	xrdxrf_scans = {
		"experiment_folder_id": "exp_01",
		"experiment_name": "experiment",
		"scans": {0: {"igsn": "IGSN-123"}, 1: {}},
	}
	calibration_model = {"metadata": {"version": "1.0", "source_file_id": "model_file"}}
	poni = {
		"poni_path": str(poni_path),
		"calibrant_scan_file_id": "cal_file",
		"calibrant_scan_file_name": "xrd_calibrant_data_000001.h5",
		"cache_hit": True,
	}
	azimuthal_integration = {
		0: pd.DataFrame({"q_nm^-1": [1.0], "intensity": [2.0]}),
		1: pd.DataFrame({"q_nm^-1": [3.0], "intensity": [4.0]}),
	}
	lattice_parameters = {
		0: pd.DataFrame({"a_nm_avg": [0.5]}),
		1: pd.DataFrame({"a_nm_avg": [0.6]}),
	}

	assets.publish_xrd_results(
		context,
		xrdxrf_scans=xrdxrf_scans,
		calibration_model=calibration_model,
		poni=poni,
		azimuthal_integration=azimuthal_integration,
		lattice_parameters=lattice_parameters,
	)

	metadata_by_item_name = {
		item["name"]: metadata
		for item_name, item in gc._items.items()
		for metadata_item_id, metadata in gc.metadata_calls
		if metadata_item_id == item["_id"]
	}

	assert metadata_by_item_name["scan_point_0_azimuthal.csv"] == {"igsn": "IGSN-123"}
	assert metadata_by_item_name["scan_point_0_lattice_parameters.csv"] == {"igsn": "IGSN-123"}
	assert "scan_point_1_azimuthal.csv" not in metadata_by_item_name
	assert "scan_point_1_lattice_parameters.csv" not in metadata_by_item_name
