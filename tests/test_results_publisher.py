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


def test_upload_result_batch_and_metadata_builders() -> None:
    uploads = []

    def _upload_fn(**kwargs):
        uploads.append(kwargs)

    results = {0: pd.DataFrame({"x": [1]}), 1: pd.DataFrame({"x": [2]})}

    uploaded = results_publisher.upload_result_batch(
        gc=object(),
        folder_id="folder",
        results=results,
        scan_metadata_getter=lambda scan_id: {"igsn": f"IGSN-{scan_id}"},
        result_type="azimuthal",
        upload_fn=_upload_fn,
    )

    assert uploaded == ["scan_point_0_azimuthal.csv", "scan_point_1_azimuthal.csv"]
    assert len(uploads) == 2
    assert uploads[0]["item_metadata"]["igsn"] == "IGSN-0"


def test_build_item_link_strips_api_suffix() -> None:
    assert (
        results_publisher.build_item_link("https://girder.example/api/v1", "item_1")
        == "https://girder.example/#item/item_1"
    )


def test_build_calibrant_metadata_omits_empty_igsn() -> None:
    payload = results_publisher.build_calibrant_metadata("cal_item", "https://girder.example/api/v1", None)

    assert payload["item_id"] == "cal_item"
    assert "igsn" not in payload
