"""
Converts PyMCA mass fractions to atomic percentages and emits CSV/plots.
"""

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import matplotlib.pyplot as plt


_PATTERN = re.compile(
    r"SOURCE:\s*scan_point_(\d+)\.mca.*?"
    r"Ti\s+K\s+[\d.eE+-]+\s+[\d.eE+-]+\s+([\d.eE+-]+).*?"
    r"Cu\s+K\s+[\d.eE+-]+\s+[\d.eE+-]+\s+([\d.eE+-]+)",
    re.DOTALL,
)


def calculate_atomic_percent_ti(ti_mass_fraction: float, cu_mass_fraction: float) -> Tuple[float, float]:
    """
    Calculate the atomic percentages of Ti and Cu from their mass fractions.

    Parameters
    ----------
    ti_mass_fraction : float
        Mass fraction of titanium (Ti).
    cu_mass_fraction : float
        Mass fraction of copper (Cu).

    Returns
    -------
    at_percent_cu : float
        Atomic percent of copper (Cu).
    at_percent_ti : float
        Atomic percent of titanium (Ti).

    Notes
    -----
    Atomic percent is calculated as:
        n_element = mass_fraction / atomic_weight
        atomic_percent_element = n_element / (n_cu + n_ti) * 100
    """
    cu_atomic_weight = 63.546
    ti_atomic_weight = 47.867

    n_cu = cu_mass_fraction / cu_atomic_weight
    n_ti = ti_mass_fraction / ti_atomic_weight
    n_total = n_cu + n_ti

    at_percent_cu = (n_cu / n_total) * 100
    at_percent_ti = (n_ti / n_total) * 100

    return at_percent_cu, at_percent_ti


def parse_pymca_text(data_text: str) -> Dict[int, Dict[str, float]]:
    """
    Parse PyMCA text output and return mass fractions by scan point.
    """
    matches = _PATTERN.findall(data_text)
    if not matches:
        return {}

    result: Dict[int, Dict[str, float]] = {}
    for scan_point, ti_frac, cu_frac in matches:
        sp = int(scan_point)
        ti_f = float(ti_frac)
        cu_f = float(cu_frac)
        result[sp] = {"ti_mass": ti_f, "cu_mass": cu_f}
    return result


def process_pymca_text_file(text_file: str) -> Tuple[str, str]:
    """
    Process a single PyMCA .txt output file and emit CSV and PNG.

    Returns (csv_path, png_path).
    """
    text_path = Path(text_file)
    data_text = text_path.read_text()
    mass_fractions = parse_pymca_text(data_text)
    if not mass_fractions:
        raise ValueError(f"No valid scan data found in {text_path}")

    ti_at_percent: Dict[int, float] = {}
    for sp, vals in mass_fractions.items():
        _, ti_at = calculate_atomic_percent_ti(vals["ti_mass"], vals["cu_mass"])
        ti_at_percent[sp] = ti_at

    sorted_points = sorted(ti_at_percent.keys())
    ti_at_values = [ti_at_percent[sp] for sp in sorted_points]

    base_name = text_path.stem
    output_png = text_path.with_name(f"{base_name}_TiAtomicPercent.png")
    output_csv = text_path.with_name(f"{base_name}_TiAtomicPercent.csv")

    plt.figure(figsize=(10, 6))
    plt.plot(sorted_points, ti_at_values, marker="^", color="tab:blue", label="Ti Atomic Percent")
    plt.xlabel("Scan Point")
    plt.ylabel("Ti Atomic Percent (%)")
    plt.title("Ti Atomic Percent")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_png)
    plt.close()

    with output_csv.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Scan Point", "Ti Mass Fraction", "Cu Mass Fraction", "Ti Atomic Percent"])
        for sp in sorted_points:
            writer.writerow([
                sp,
                mass_fractions[sp]["ti_mass"],
                mass_fractions[sp]["cu_mass"],
                ti_at_percent[sp],
            ])

    return str(output_csv), str(output_png)


def process_pymca_directory(root_dir: str) -> List[Tuple[str, str]]:
    """
    Process all PyMCA .txt output files under root_dir.

    Returns a list of (csv_path, png_path) tuples.
    """
    outputs: List[Tuple[str, str]] = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if not filename.lower().endswith(".txt"):
                continue
            full_path = os.path.join(dirpath, filename)
            outputs.append(process_pymca_text_file(full_path))

    return outputs


__all__ = [
    "calculate_atomic_percent_ti",
    "parse_pymca_text",
    "process_pymca_text_file",
    "process_pymca_directory",
]
