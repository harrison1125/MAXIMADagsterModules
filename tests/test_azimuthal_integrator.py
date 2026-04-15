from pathlib import Path

import numpy as np
import pytest

pyfai = pytest.importorskip("pyFAI")
fabio = pytest.importorskip("fabio")

from MaximaDagster.modules import AzimuthalIntegrator as az


class _FakeAI:
    def integrate1d(self, image, npt=10000, unit=None, radial_range=None):
        q = np.linspace(20.0, 55.0, int(npt), dtype=float)
        intensity = np.linspace(1.0, 100.0, int(npt), dtype=float)
        return q, intensity


def test_integrate_pattern_returns_dataframe(monkeypatch):
    fake_ai = _FakeAI()

    image = np.ones((8, 8), dtype=np.uint32)
    result = az.integrate_pattern(image=image, ai=fake_ai, npt=5)

    assert list(result.columns) == [az.Q_COLUMN, az.INTENSITY_COLUMN]
    assert len(result) == 5
    assert result[az.Q_COLUMN].iloc[0] == pytest.approx(20.0)


def test_integrate_dict_returns_mapping_of_dataframes(monkeypatch):
    monkeypatch.setattr(az, "_create_integrator_from_geometry", lambda geometry: _FakeAI())

    scans = {
        0: np.ones((4, 4), dtype=np.uint32),
        1: np.ones((4, 4), dtype=np.uint32) * 2,
    }
    result = az.integrate_dict(scans, geometry=object(), npt=3)

    assert set(result.keys()) == {0, 1}
    assert all(list(df.columns) == [az.Q_COLUMN, az.INTENSITY_COLUMN] for df in result.values())
    assert all(len(df) == 3 for df in result.values())


def test_integrate_directory_uses_run_integration(tmp_path: Path, monkeypatch):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    image_a = input_dir / "a.tif"
    image_b = input_dir / "b.tiff"
    image_other = input_dir / "c.jpg"
    image_a.write_text("")
    image_b.write_text("")
    image_other.write_text("")

    output_dir = tmp_path / "out"

    def fake_run_integration(image_path, poni_file, output_dir=None, npt=10000, x_limits=None, y_limits=None):
        stem = Path(image_path).stem
        dat_path = Path(output_dir) / f"{stem}.dat"
        png_path = Path(output_dir) / f"{stem}.png"
        dat_path.write_text("")
        png_path.write_text("")
        return str(dat_path), str(png_path)

    monkeypatch.setattr(az, "run_integration", fake_run_integration)

    results = az.integrate_directory(
        input_directory=str(input_dir),
        poni_file=str(tmp_path / "test.poni"),
        output_directory=str(output_dir),
    )

    assert set(results.keys()) == {"a", "b"}
    assert Path(results["a"]["dat"]).exists()
    assert Path(results["b"]["png"]).exists()


@pytest.mark.parametrize(
    "bad_image",
    [
        np.ones((2, 2, 2), dtype=np.float32),
        np.array([], dtype=np.float32),
        np.array([["a", "b"], ["c", "d"]], dtype=object),
    ],
)
def test_integrate_pattern_validates_bad_input_shapes_and_types(bad_image: np.ndarray) -> None:
    fake_ai = _FakeAI()

    with pytest.raises(ValueError):
        az.integrate_pattern(image=bad_image, ai=fake_ai, npt=5)


def test_integrate_pattern_retries_without_unit_when_ai_rejects_keyword() -> None:
    class _UnitRejectingAI:
        def __init__(self) -> None:
            self.calls = 0

        def integrate1d(self, image, **kwargs):
            self.calls += 1
            if "unit" in kwargs:
                raise TypeError("unexpected keyword argument: unit")
            q = np.linspace(10.0, 20.0, int(kwargs["npt"]), dtype=float)
            i = np.linspace(1.0, 2.0, int(kwargs["npt"]), dtype=float)
            return q, i

    ai = _UnitRejectingAI()
    result = az.integrate_pattern(image=np.ones((4, 4), dtype=np.float32), ai=ai, npt=4)

    assert ai.calls == 2
    assert list(result.columns) == [az.Q_COLUMN, az.INTENSITY_COLUMN]
    assert len(result) == 4
