from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from MaximaDagster.modules import LatticeParameters as lattice


def _make_integrated_scan_df() -> pd.DataFrame:
    q_vals = np.array([28.0, 29.5, 31.0, 34.5, 35.5, 48.5, 50.0], dtype=float)
    intensity = np.array([2.0, 50.0, 3.0, 60.0, 4.0, 70.0, 5.0], dtype=float)
    return pd.DataFrame({"q_nm^-1": q_vals, "intensity": intensity})


def test_process_scan_dataframe_returns_single_row():
    scan_df = _make_integrated_scan_df()

    result = lattice.process_scan_dataframe(scan_df=scan_df, scan_id=7)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.loc[0, "scan_point"] == 7
    assert pd.notna(result.loc[0, "Qmax_29.0_32.0"])
    assert pd.notna(result.loc[0, "Qmax_34.0_36.0"])
    assert pd.notna(result.loc[0, "Qmax_47.0_51.0"])
    assert pd.notna(result.loc[0, "a_nm_avg"])
    assert pd.notna(result.loc[0, "a_A_avg"])


def test_process_integrated_dict_returns_dataframe_per_scan():
    integrated = {
        0: _make_integrated_scan_df(),
        1: _make_integrated_scan_df(),
    }

    result = lattice.process_integrated_dict(integrated)

    assert set(result.keys()) == {0, 1}
    assert all(isinstance(df, pd.DataFrame) for df in result.values())
    assert all(len(df) == 1 for df in result.values())


def test_process_folder_legacy_wrapper(tmp_path: Path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    data = np.column_stack(
        (
            np.array([28.0, 29.5, 31.0, 34.5, 35.5, 48.5, 50.0], dtype=float),
            np.array([2.0, 50.0, 3.0, 60.0, 4.0, 70.0, 5.0], dtype=float),
        )
    )

    for i in range(2):
        file_path = data_dir / f"scan_point_{i}.dat"
        np.savetxt(file_path, data, header="Q Intensity", comments="")

    def fake_to_excel(self, path, index=False):
        Path(path).write_text("excel")

    monkeypatch.setattr(pd.DataFrame, "to_excel", fake_to_excel, raising=False)

    xlsx_path, png_path = lattice.process_folder(str(data_dir))

    assert Path(xlsx_path).exists()
    assert Path(png_path).exists()


def test_process_scan_dataframe_validates_ranges():
    scan_df = _make_integrated_scan_df()

    with pytest.raises(ValueError):
        lattice.process_scan_dataframe(
            scan_df=scan_df,
            q_ranges=[(1.0, 2.0)],
            hkl_ranges=[(1, 1, 1), (2, 0, 0)],
        )
