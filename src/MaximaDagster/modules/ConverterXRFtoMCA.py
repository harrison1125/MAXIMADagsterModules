"""
Converts .xrf files from the XRF detector into PyMCA-compatible .mca files.

Notes
-----
- This uses a simple line-by-line parse and may need updates for future XRF
  format changes.
- Counts are extracted from the second column of each data line.
- Header lines containing 'xrf_data' are skipped.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional


def _parse_xrf_counts(lines: Iterable[str]) -> List[int]:
    counts: List[int] = []
    for line in lines:
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        if parts[0] == "xrf_data":
            continue
        try:
            count = int(float(parts[1]))
        except ValueError:
            continue
        counts.append(count)
    return counts


def convert_xrf_to_mca(
    input_file: Optional[str] = None,
    output_file: Optional[str] = None,
    *,
    input_path: Optional[str] = None,
    output_path: Optional[str] = None,
    h5_path: Optional[str] = None,
    cfg_path: Optional[str] = None,
    output: Optional[str] = None,
) -> str:
    """
    Convert an XRF detector file (.xrf) to a PyMCA-compatible MCA file (.mca).

    Parameters
    ----------
    input_file : str, optional
        Path to the input .xrf file.
    output_file : str, optional
        Path where the output .mca file will be saved.
    input_path : str, optional
        Alias for input_file.
    output_path : str, optional
        Alias for output_file.

    Returns
    -------
    str
        Path to the written .mca file.
    """
    _ = cfg_path

    src = Path(input_path or input_file or h5_path or "")
    dst = Path(output_path or output_file or output or "")

    if not src:
        raise ValueError("input_file or input_path is required")
    if not dst:
        raise ValueError("output_file or output_path is required")

    lines = src.read_text().splitlines()
    counts = _parse_xrf_counts(lines)

    mca_lines = [
        "<<PMCA SPECTRUM>>",
        "VERSION: 1.0",
        f"CHANNELS: {len(counts)}",
        "<<DATA>>",
        *[str(count) for count in counts],
        "<<END>>",
    ]

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text("\n".join(mca_lines))

    return str(dst)


def convert_xrf_folder(
    input_directory: str,
    output_directory: Optional[str] = None,
    pattern: str = "*.xrf",
) -> List[str]:
    """
    Convert all matching .xrf files in a folder (recursively) to .mca files.

    Returns a list of output file paths.
    """
    src_dir = Path(input_directory)
    dst_dir = Path(output_directory) if output_directory else src_dir

    outputs: List[str] = []
    for src in src_dir.rglob(pattern):
        rel = src.relative_to(src_dir)
        out_path = dst_dir / rel.with_suffix(".mca")
        outputs.append(convert_xrf_to_mca(input_path=str(src), output_path=str(out_path)))

    return outputs


__all__ = [
    "convert_xrf_to_mca",
    "convert_xrf_folder",
]
