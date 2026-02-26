"""
Lattice parameter extraction from XRD/XRF scan data.
"""

import os
import glob
import re
from typing import Dict, List, Mapping, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression


DEFAULT_Q_RANGES: List[Tuple[float, float]] = [
    (29.0, 32.0),
    (34.0, 36.0),
    (47.0, 51.0),
]

DEFAULT_HKL_RANGES: List[Tuple[int, int, int]] = [
    (1, 1, 1),
    (2, 0, 0),
    (2, 2, 0),
]

Q_COLUMN = "q_nm^-1"
INTENSITY_COLUMN = "intensity"

def extract_scan_number(file_path: str) -> int:
    """
    Extract scan-point index from filename.

    Parameters
    ----------
    file_path : str
        Full path to a scan_point_*.dat file.

    Returns
    -------
    int
        Scan-point index. Returns -1 if not found.
    """
    match = re.search(r"scan_point_(\d+)\.dat", file_path)
    return int(match.group(1)) if match else -1


def find_peak_q(
    q: np.ndarray,
    intensity: np.ndarray,
    q_min: float,
    q_max: float,
) -> float:
    """
    Find Q position of maximum intensity within a given Q range.

    Parameters
    ----------
    q : np.ndarray
        Q values.
    intensity : np.ndarray
        Intensity values.
    q_min : float
        Lower bound of Q range.
    q_max : float
        Upper bound of Q range.

    Returns
    -------
    float
        Q position of peak maximum. NaN if range is empty.
    """
    mask = (q >= min(q_min, q_max)) & (q <= max(q_min, q_max))
    if not np.any(mask):
        return np.nan

    idx = np.argmax(intensity[mask])
    return q[mask][idx]


def _validate_ranges(
    q_ranges: List[Tuple[float, float]],
    hkl_ranges: List[Tuple[int, int, int]],
) -> None:
    if len(q_ranges) != len(hkl_ranges):
        raise ValueError("q_ranges and hkl_ranges must be the same length")


def process_scan_dataframe(
    scan_df: pd.DataFrame,
    q_ranges: Optional[List[Tuple[float, float]]] = None,
    hkl_ranges: Optional[List[Tuple[int, int, int]]] = None,
    scan_id: Optional[object] = None,
) -> pd.DataFrame:
    """
    Compute lattice parameters for a single integrated scan in memory.

    Parameters
    ----------
    scan_df : pd.DataFrame
        Integrated scan with Q and intensity columns.
    q_ranges : list[tuple[float, float]], optional
        Q ranges for peak picking.
    hkl_ranges : list[tuple[int, int, int]], optional
        HKL assignments corresponding to `q_ranges`.
    scan_id : object, optional
        Scan identifier to include in output.

    Returns
    -------
    pd.DataFrame
        Single-row DataFrame containing peak positions, d-spacings,
        and fitted lattice parameter.
    """
    q_ranges = q_ranges or DEFAULT_Q_RANGES
    hkl_ranges = hkl_ranges or DEFAULT_HKL_RANGES
    _validate_ranges(q_ranges, hkl_ranges)

    if scan_df.empty:
        raise ValueError("scan_df is empty")

    if Q_COLUMN not in scan_df.columns:
        raise ValueError(f"Required column '{Q_COLUMN}' not found in scan_df")
    if INTENSITY_COLUMN not in scan_df.columns:
        raise ValueError(f"Required column '{INTENSITY_COLUMN}' not found in scan_df")

    q_vals = pd.to_numeric(scan_df[Q_COLUMN], errors="coerce").to_numpy(dtype=float)
    intensity_vals = pd.to_numeric(scan_df[INTENSITY_COLUMN], errors="coerce").to_numpy(dtype=float)

    record = {"scan_point": scan_id}
    inv_sqrt_hkl: List[float] = []
    d_columns: List[str] = []

    for (q_min, q_max), (h, k, l) in zip(q_ranges, hkl_ranges):
        qmax_col = f"Qmax_{q_min:.1f}_{q_max:.1f}"
        d_col = f"d_nm_{q_min:.1f}_{q_max:.1f}"
        q_peak = find_peak_q(q_vals, intensity_vals, q_min, q_max)
        record[qmax_col] = q_peak
        record[d_col] = 2.0 * np.pi / q_peak if pd.notna(q_peak) and q_peak != 0 else np.nan

        d_columns.append(d_col)
        inv_sqrt_hkl.append(1.0 / np.sqrt(h**2 + k**2 + l**2))

    d_vals = np.asarray([record[col] for col in d_columns], dtype=float)
    valid = ~np.isnan(d_vals)

    if np.sum(valid) >= 2:
        X = np.asarray(inv_sqrt_hkl, dtype=float)[valid].reshape(-1, 1)
        y = d_vals[valid]
        model = LinearRegression(fit_intercept=False)
        model.fit(X, y)
        a_nm = float(model.coef_[0])
    else:
        a_nm = np.nan

    record["a_nm_avg"] = a_nm
    record["a_A_avg"] = a_nm * 10.0 if pd.notna(a_nm) else np.nan
    return pd.DataFrame([record])


def process_integrated_dict(
    integrated_scans: Mapping[int, pd.DataFrame],
    q_ranges: Optional[List[Tuple[float, float]]] = None,
    hkl_ranges: Optional[List[Tuple[int, int, int]]] = None,
) -> Dict[int, pd.DataFrame]:
    """
    Process a mapping of integrated scans and return one result frame per scan.

    Parameters
    ----------
    integrated_scans : Mapping[int, pd.DataFrame]
        Mapping of scan ID to integrated intensity profile.

    Returns
    -------
    Dict[int, pd.DataFrame]
        Mapping of scan ID to one-row DataFrame containing lattice parameters.
    """
    outputs: Dict[int, pd.DataFrame] = {}
    for scan_id, scan_df in integrated_scans.items():
        outputs[int(scan_id)] = process_scan_dataframe(
            scan_df=scan_df,
            q_ranges=q_ranges,
            hkl_ranges=hkl_ranges,
            scan_id=scan_id,
        )
    return outputs


# =============================================================================
# CORE PROCESSING
# =============================================================================

def process_folder(
    folder_path: str,
    q_ranges: List[Tuple[float, float]] = None,
    hkl_ranges: List[Tuple[int, int, int]] = None,
    scan_slice: slice = slice(None),
    output_xlsx_path: str = None,
    output_png_path: str = None,
) -> Tuple[str, str]:
    """
    Process all scan_point_*.dat files in a folder and compute
    average lattice parameters.

    Parameters
    ----------
    folder_path : str
        Path to the folder containing scan_point_*.dat files.

    Notes
    -----
    - Assumes cubic symmetry:
        d_hkl = a / sqrt(h^2 + k^2 + l^2)
    - Linear regression is performed with zero intercept.
    """
    q_ranges = q_ranges or DEFAULT_Q_RANGES
    hkl_ranges = hkl_ranges or DEFAULT_HKL_RANGES
    _validate_ranges(q_ranges, hkl_ranges)

    file_pattern = os.path.join(folder_path, "scan_point_*.dat")
    files = sorted(glob.glob(file_pattern), key=extract_scan_number)
    files = files[scan_slice]

    if not files:
        raise FileNotFoundError(f"No .dat files found in {folder_path}")

    records = []

    # -------------------------------------------------------------------------
    # In-memory peak extraction + lattice fit per scan
    # -------------------------------------------------------------------------
    for file_path in files:
        data = np.loadtxt(file_path, skiprows=1)
        integrated_df = pd.DataFrame({"q_nm^-1": data[:, 0], "intensity": data[:, 1]})
        scan_name = os.path.basename(file_path)
        row_df = process_scan_dataframe(
            scan_df=integrated_df,
            q_ranges=q_ranges,
            hkl_ranges=hkl_ranges,
            scan_id=scan_name,
        )
        records.append(row_df.iloc[0].to_dict())

    df = pd.DataFrame(records)

    # -------------------------------------------------------------------------
    # Output
    # -------------------------------------------------------------------------
    folder_name = os.path.basename(os.path.normpath(folder_path))
    output_xlsx = (
        output_xlsx_path
        if output_xlsx_path
        else os.path.join(folder_path, f"{folder_name}_lattice_parameters.xlsx")
    )
    df.to_excel(output_xlsx, index=False)

    # -------------------------------------------------------------------------
    # Plot
    # -------------------------------------------------------------------------
    plt.figure(figsize=(10, 4))
    plt.plot(df["a_A_avg"], marker="o", linewidth=1.5)
    plt.xticks(range(len(df)), df["scan_point"], rotation=90)
    plt.xlabel("Scan Point")
    plt.ylabel("Average Lattice Parameter a (A)")
    plt.title(f"Average FCC Lattice Parameter - {folder_name}")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    output_png = (
        output_png_path
        if output_png_path
        else os.path.join(folder_path, f"{folder_name}_lattice_parameters.png")
    )
    plt.savefig(output_png)
    plt.close()

    return output_xlsx, output_png


def process_folders(
    folder_paths: List[str],
    q_ranges: List[Tuple[float, float]] = None,
    hkl_ranges: List[Tuple[int, int, int]] = None,
    scan_slice: slice = slice(None),
) -> List[Tuple[str, str]]:
    """
    Process multiple folders and return list of (xlsx, png) outputs.
    """
    outputs: List[Tuple[str, str]] = []
    for path in folder_paths:
        outputs.append(
            process_folder(
                folder_path=path,
                q_ranges=q_ranges,
                hkl_ranges=hkl_ranges,
                scan_slice=scan_slice,
            )
        )
    return outputs


__all__ = [
    "DEFAULT_Q_RANGES",
    "DEFAULT_HKL_RANGES",
    "process_scan_dataframe",
    "process_integrated_dict",
    "extract_scan_number",
    "find_peak_q",
    "process_folder",
    "process_folders",
]
