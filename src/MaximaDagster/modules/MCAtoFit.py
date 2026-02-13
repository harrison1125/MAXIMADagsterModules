"""
Batch process .mca files using PyMCA.

This module exposes callable functions suitable for Dagster assets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional
import subprocess


def _collect_mca_files(folder: Path) -> List[str]:
    return [str(p) for p in folder.glob("*.mca") if p.is_file()]


def run_pymca_batch(
    root_dir: str,
    config_path: str,
    output_root: Optional[str] = None,
    concentrations: bool = True,
) -> List[str]:
    """
    Recursively process all .mca files under root_dir using pymcabatch.

    Returns a list of directories that were processed.
    """
    processed_dirs: List[str] = []
    root_path = Path(root_dir)
    output_root_path = Path(output_root) if output_root else None

    for dirpath in [p for p in root_path.rglob("*") if p.is_dir()] + [root_path]:
        mca_files = _collect_mca_files(dirpath)
        if not mca_files:
            continue

        out_dir = output_root_path / dirpath.relative_to(root_path) if output_root_path else dirpath
        out_dir.mkdir(parents=True, exist_ok=True)

        command = [
            "pymcabatch",
            f"--cfg={config_path}",
            f"--outdir={out_dir}",
            "--exitonend=1",
        ]
        if concentrations:
            command.append("--concentrations=1")
        command += mca_files

        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"pymcabatch failed in {dirpath} with code {result.returncode}: {result.stderr}"
            )

        processed_dirs.append(str(out_dir))

    return processed_dirs


__all__ = [
    "run_pymca_batch",
]
