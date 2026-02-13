from pathlib import Path

import pytest

pyfai = pytest.importorskip("pyFAI")
fabio = pytest.importorskip("fabio")

from MaximaDagster.modules import AzimuthalIntegrator as az


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
