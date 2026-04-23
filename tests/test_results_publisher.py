import pandas as pd

from MaximaDagster.utils import results_publisher


def test_get_workflow_version_uses_cache_after_first_read(monkeypatch) -> None:
    monkeypatch.setattr(results_publisher, "_WORKFLOW_VERSION_CACHE", None)

    first = results_publisher.get_workflow_version()
    second = results_publisher.get_workflow_version()

    assert first
    assert second == first


def test_get_workflow_version_returns_unknown_on_read_error(monkeypatch) -> None:
    class _BrokenPath:
        @staticmethod
        def resolve():
            raise RuntimeError("fs error")

    monkeypatch.setattr(results_publisher, "Path", _BrokenPath)
    monkeypatch.setattr(results_publisher, "_WORKFLOW_VERSION_CACHE", None)

    assert results_publisher.get_workflow_version() == "unknown"


class _FakeUploadClient:
    def __init__(self, existing_files):
        self.existing_files = list(existing_files)
        self.calls = []

    def loadOrCreateItem(self, filename, folder_id):
        self.calls.append(("loadOrCreateItem", filename, folder_id))
        return {"_id": "item_1"}

    def get(self, route, parameters=None):
        self.calls.append(("get", route, parameters))
        assert route == "item/item_1/files"
        return list(self.existing_files)

    def uploadFileContents(self, file_id, stream, size):
        self.calls.append(("uploadFileContents", file_id, size, stream.read()))
        return {"_id": file_id}

    def post(self, route, parameters=None):
        self.calls.append(("post", route, parameters))
        assert route == "file"
        return {"_id": "file_meta_1"}

    def _uploadContents(self, file_meta, stream, size):
        self.calls.append(("_uploadContents", file_meta["_id"], size, stream.read()))
        return {"_id": "uploaded_file_1"}

    def addMetadataToItem(self, item_id, metadata):
        self.calls.append(("addMetadataToItem", item_id, metadata))


def test_upload_artifact_creates_file_when_item_has_no_files() -> None:
    gc = _FakeUploadClient(existing_files=[])

    item_id = results_publisher.upload_artifact(
        gc=gc,
        folder_id="folder_1",
        filename="artifact.csv",
        payload=b"a,b\n1,2\n",
        mime_type="text/csv",
        metadata={"k": "v"},
    )

    assert item_id == "item_1"
    assert any(call[0] == "post" for call in gc.calls)
    assert any(call[0] == "_uploadContents" for call in gc.calls)
    assert any(call[0] == "addMetadataToItem" and call[2] == {"k": "v"} for call in gc.calls)


def test_upload_artifact_updates_existing_file_when_present() -> None:
    gc = _FakeUploadClient(existing_files=[{"_id": "file_existing_1"}])

    item_id = results_publisher.upload_artifact(
        gc=gc,
        folder_id="folder_1",
        filename="artifact.csv",
        payload=b"a,b\n3,4\n",
        mime_type="text/csv",
        metadata={"x": 1},
    )

    assert item_id == "item_1"
    assert any(call[0] == "uploadFileContents" and call[1] == "file_existing_1" for call in gc.calls)
    assert not any(call[0] == "post" for call in gc.calls)
    assert any(call[0] == "addMetadataToItem" and call[2] == {"x": 1} for call in gc.calls)


def test_build_item_link_strips_api_suffix() -> None:
    assert (
        results_publisher.build_item_link("https://girder.example/api/v1", "item_1")
        == "https://girder.example/#item/item_1"
    )


def test_build_calibrant_metadata_omits_empty_igsn() -> None:
    payload = results_publisher.build_calibrant_metadata("cal_item", "https://girder.example/api/v1", None)

    assert payload["item_id"] == "cal_item"
    assert "igsn" not in payload


def test_build_poni_linkage_metadata_has_exact_shape() -> None:
    geometry = pd.Series(
        {
            "dist": 1.1,
            "poni1": 2.2,
            "poni2": 3.3,
            "rot1": 4.4,
            "rot2": 5.5,
            "rot3": 6.6,
        }
    )

    class _GeometryProxy:
        dist = float(geometry["dist"])
        poni1 = float(geometry["poni1"])
        poni2 = float(geometry["poni2"])
        rot1 = float(geometry["rot1"])
        rot2 = float(geometry["rot2"])
        rot3 = float(geometry["rot3"])

    payload = results_publisher.build_poni_linkage_metadata(
        poni_item_id="poni_item_1",
        girder_url="https://girder.example/api/v1",
        geometry=_GeometryProxy(),
    )

    assert set(payload.keys()) == {"item_id", "link", "geometry"}
    assert payload["item_id"] == "poni_item_1"
    assert payload["link"] == "https://girder.example/#item/poni_item_1"
    assert set(payload["geometry"].keys()) == {"dist", "poni1", "poni2", "rot1", "rot2", "rot3"}
