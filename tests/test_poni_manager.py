from pathlib import Path

from MaximaDagster.utils import poni_manager


def test_load_index_returns_empty_when_file_missing(tmp_path: Path) -> None:
    cache = poni_manager.CalibrationCache(cache_dir=tmp_path)

    assert cache.load_index() == {}


def test_save_and_load_index_roundtrip(tmp_path: Path) -> None:
    cache = poni_manager.CalibrationCache(cache_dir=tmp_path)
    payload = {"cal_1": {"poni_path": str(tmp_path / "cal_1.poni")}}

    cache.save_index(payload)

    assert cache.load_index() == payload


def test_validate_entry_checks_file_and_model_identity(tmp_path: Path) -> None:
    cache = poni_manager.CalibrationCache(cache_dir=tmp_path)
    poni_path = tmp_path / "a.poni"
    poni_path.write_text("poni")
    entry = {
        "poni_path": str(poni_path),
        "model_version": "1.0",
        "model_source_file_id": "model_file",
    }

    assert cache.validate_entry(entry, "1.0", "model_file") is True
    assert cache.validate_entry(entry, "2.0", "model_file") is False
    assert cache.validate_entry(entry, "1.0", "other_file") is False


def test_get_entry_for_calibrant_returns_valid_cache_entry(tmp_path: Path) -> None:
    cache = poni_manager.CalibrationCache(cache_dir=tmp_path)
    poni_path = tmp_path / "a.poni"
    poni_path.write_text("poni")
    cache.save_index(
        {
            "cal_1": {
                "poni_path": str(poni_path),
                "calibrant_scan_file_id": "cal_1",
                "calibrant_scan_file_name": "xrd_calibrant_data_000001.h5",
                "calibrant_scan_updated": "2026-03-12T10:00:00.000+00:00",
                "model_version": "1.0",
                "model_source_file_id": "model_file",
                "updated_at": "2026-03-12T11:00:00.000+00:00",
            }
        }
    )

    entry = cache.get_entry_for_calibrant("cal_1", "1.0", "model_file")

    assert entry is not None
    assert entry.calibrant_scan_file_id == "cal_1"
    assert entry.poni_path == poni_path


def test_save_entry_persists_calibrant_metadata(tmp_path: Path) -> None:
    cache = poni_manager.CalibrationCache(cache_dir=tmp_path)
    poni_path = tmp_path / "new.poni"
    poni_path.write_text("poni")

    cache.save_entry(
        calibrant_file_id="cal_9",
        poni_path=poni_path,
        calibrant_scan_file_name="xrd_calibrant_data_000009.h5",
        calibrant_scan_updated="2026-03-15T10:00:00.000+00:00",
        model_version="2.0",
        model_source_file_id="model_2",
    )

    index = cache.load_index()

    assert index["cal_9"]["calibrant_scan_file_name"] == "xrd_calibrant_data_000009.h5"
    assert index["cal_9"]["model_version"] == "2.0"
    assert "updated_at" in index["cal_9"]


def test_load_geometry_from_poni_uses_pyfai_integrator(monkeypatch) -> None:
    class _FakeIntegrator:
        def __init__(self) -> None:
            self.loaded = None
            self.dist = 1.1
            self.poni1 = 2.2
            self.poni2 = 3.3
            self.rot1 = 4.4
            self.rot2 = 5.5
            self.rot3 = 6.6
            self.detector = "detector"
            self.wavelength = 7.7

        def load(self, path: str) -> None:
            self.loaded = path

    monkeypatch.setattr(poni_manager, "PyFAIAzimuthalIntegrator", _FakeIntegrator)

    geometry = poni_manager.load_geometry_from_poni("calibration.poni")

    assert geometry.dist == 1.1
    assert geometry.poni1 == 2.2
    assert geometry.poni2 == 3.3
    assert geometry.rot1 == 4.4
    assert geometry.rot2 == 5.5
    assert geometry.rot3 == 6.6
    assert geometry.wavelength == 7.7
