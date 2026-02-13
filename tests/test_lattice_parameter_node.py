import os
from pathlib import Path
import importlib.machinery
import importlib.util

import numpy as np
import pytest

os.environ.setdefault("MPLBACKEND", "Agg")

pytest.importorskip("pandas")
pytest.importorskip("sklearn")
pytest.importorskip("matplotlib")


def load_module_from_path(path: Path):
    loader = importlib.machinery.SourceFileLoader("dag_lattice", str(path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_process_folder(tmp_path: Path, monkeypatch):
    module_path = Path(__file__).resolve().parents[1] / "src" / "MaximaDagster" / "modules" / "DAG_Lattice_Parameter_Node"
    lattice = load_module_from_path(module_path)

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    q_values = np.array([29.5, 34.5, 48.5])
    intensities = np.array([10.0, 20.0, 30.0])
    data = np.column_stack((q_values, intensities))

    for i in range(2):
        file_path = data_dir / f"scan_point_{i}.dat"
        np.savetxt(file_path, data, header="Q Intensity", comments="")

    def fake_to_excel(self, path, index=False):
        Path(path).write_text("excel")

    monkeypatch.setattr(lattice.pd.DataFrame, "to_excel", fake_to_excel, raising=False)

    xlsx_path, png_path = lattice.process_folder(str(data_dir))

    assert Path(xlsx_path).exists()
    assert Path(png_path).exists()
