import os
from pathlib import Path

import pytest

os.environ.setdefault("MPLBACKEND", "Agg")

from MaximaDagster.modules import concentrations


def test_parse_pymca_text():
    text = (
        "SOURCE: scan_point_1.mca\n"
        "Ti  K  0  0  0.12\n"
        "Cu  K  0  0  0.88\n"
    )
    parsed = concentrations.parse_pymca_text(text)
    assert parsed[1]["ti_mass"] == 0.12
    assert parsed[1]["cu_mass"] == 0.88


def test_process_pymca_text_file(tmp_path: Path):
    text = (
        "SOURCE: scan_point_1.mca\n"
        "Ti  K  0  0  0.1\n"
        "Cu  K  0  0  0.9\n"
    )
    text_path = tmp_path / "output.txt"
    text_path.write_text(text)

    csv_path, png_path = concentrations.process_pymca_text_file(str(text_path))

    assert Path(csv_path).exists()
    assert Path(png_path).exists()
