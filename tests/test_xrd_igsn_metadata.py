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
	def __init__(self, h5_path: Path):
		self._sources = {
			"file_h5": h5_path,
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

	def getFile(self, file_id):
		if file_id == "cal_file":
			return {"_id": "cal_file", "itemId": "cal_item"}
		if file_id == "model_file":
			return {"_id": "model_file", "itemId": "model_item"}
		raise AssertionError(file_id)

	def getItem(self, item_id):
		if item_id == "cal_item":
			return {"_id": "cal_item", "meta": {"igsn": "CAL-IGSN-1"}}
		raise AssertionError(item_id)


class _GeometryStub:
	dist = 1.1
	poni1 = 2.2
	poni2 = 3.3
	rot1 = 4.4
	rot2 = 5.5
	rot3 = 6.6


def test_xrdxrf_scans_passes_igsn_through(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)

	h5_path = tmp_path / "source.h5"
	_write_test_h5(h5_path)

	gc = _FakeXrdGirderClient(h5_path=h5_path)
	context = build_asset_context(resources={"GirderClient": gc}, partition_key="exp_01")

	result = assets.xrdxrf_scans(context)

	assert result["scans"][0]["igsn"] == "IGSN-123"


def test_publish_xrd_results_adds_igsn_metadata_to_csv_items(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)
	monkeypatch.setenv("GIRDER_API_URL", "https://girder.example/api/v1")
	monkeypatch.setenv("GIRDER_MODEL_ITEM_ID", "model_item")

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
		"geometry": _GeometryStub(),
		"calibrant_scan_file_id": "cal_file",
		"calibrant_scan_item_id": "cal_item",
		"calibrant_scan_file_name": "xrd_calibrant_data_000001.h5",
		"cache_hit": True,
	}
	azimuthal_integration = {
		0: pd.DataFrame({"q_nm^-1": [1.0], "intensity": [2.0]}),
		1: pd.DataFrame({"q_nm^-1": [3.0], "intensity": [4.0]}),
	}

	assets.publish_xrd_results(
		context,
		xrdxrf_scans=xrdxrf_scans,
		calibration_model=calibration_model,
		poni=poni,
		azimuthal_integration=azimuthal_integration,
	)

	metadata_by_item_name = {
		item["name"]: metadata
		for item_name, item in gc._items.items()
		for metadata_item_id, metadata in gc.metadata_calls
		if metadata_item_id == item["_id"]
	}

	poni_metadata = metadata_by_item_name["calibration.poni"]
	assert set(poni_metadata.keys()) == {"prov", "model", "calibrant", "cache_hit"}
	assert poni_metadata["cache_hit"] is True
	assert poni_metadata["model"] == {
		"version": "1.0",
		"item_id": "model_item",
		"link": "https://girder.example/#item/model_item",
	}
	assert poni_metadata["calibrant"] == {
		"item_id": "cal_item",
		"link": "https://girder.example/#item/cal_item",
		"igsn": "CAL-IGSN-1",
	}

	az0 = metadata_by_item_name["scan_point_0_azimuthal.csv"]
	assert az0["igsn"] == "IGSN-123"
	assert set(az0.keys()) == {"prov", "poni", "igsn"}
	assert az0["poni"] == {
		"item_id": gc._items["calibration.poni"]["_id"],
		"link": f"https://girder.example/#item/{gc._items['calibration.poni']['_id']}",
		"geometry": {
			"dist": 1.1,
			"poni1": 2.2,
			"poni2": 3.3,
			"rot1": 4.4,
			"rot2": 5.5,
			"rot3": 6.6,
		},
	}

	az1 = metadata_by_item_name["scan_point_1_azimuthal.csv"]
	assert set(az1.keys()) == {"prov", "poni"}
	assert "igsn" not in az1

	assert "xrd_run_manifest.json" not in gc._items
	assert "scan_point_0_lattice_parameters.csv" not in gc._items
	assert "scan_point_1_lattice_parameters.csv" not in gc._items


class _FakeDatafilesXrdGirderClient:
	def __init__(self, h5_path: Path):
		self._sources = {
			"file_h5": h5_path,
		}

	def getFolder(self, folder_id):
		if folder_id == "exp_01":
			return {"_id": folder_id, "name": "exp_name"}
		if folder_id == "raw_01":
			return {"_id": "raw_01", "parentId": "exp_01", "name": "raw"}
		raise AssertionError(folder_id)

	def get(self, route, parameters=None):
		if route != "aimdl/datafiles":
			return []

		if (parameters or {}).get("dataType") != "xrd_raw":
			return []

		rows = [
			{
				"_id": "item_h5",
				"name": "scan_point_0_data_00001.h5",
				"created": "2026-03-12T10:00:00.000+00:00",
				"folderId": "raw_01",
				"experimentFolderId": "exp_01",
				"meta": {"data_type": "xrd_raw", "igsn": "IGSN-123"},
			},
			{
				"_id": "item_other_exp",
				"name": "scan_point_99_data_00001.h5",
				"created": "2026-03-12T10:00:00.000+00:00",
				"folderId": "raw_other",
				"experimentFolderId": "exp_other",
				"meta": {"data_type": "xrd_raw", "igsn": "OTHER-IGSN"},
			},
		]
		limit = int((parameters or {}).get("limit", len(rows)))
		offset = int((parameters or {}).get("offset", 0))
		return rows[offset : offset + limit]

	def listFile(self, item_id):
		if item_id == "item_h5":
			return [{"_id": "file_h5", "name": "scan_point_0_data_00001.h5"}]
		if item_id == "item_other_exp":
			return [{"_id": "file_other", "name": "scan_point_99_data_00001.h5"}]
		return []

	def downloadFile(self, file_id, local_path):
		Path(local_path).write_bytes(self._sources[file_id].read_bytes())


def test_xrdxrf_scans_datafiles_backend_filters_and_collects_sources(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)
	monkeypatch.setenv("DISCOVERY_BACKEND", "datafiles")

	h5_path = tmp_path / "source.h5"
	_write_test_h5(h5_path)

	gc = _FakeDatafilesXrdGirderClient(h5_path=h5_path)
	context = build_asset_context(resources={"GirderClient": gc}, partition_key="exp_01")

	result = assets.xrdxrf_scans(context)

	assert result["scans"][0]["igsn"] == "IGSN-123"
	assert set(result["scans"][0]["source_files"]) == {"scan_point_0_data_00001.h5"}
	assert set(result["scans"][0]["source_file_ids"]) == {"file_h5"}
	assert "xrd" in result["scans"][0]
	assert "xrf" not in result["scans"][0]
