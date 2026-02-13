from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import fabio
import numpy as np
import matplotlib.pyplot as plt
from pyFAI.integrator.azimuthal import AzimuthalIntegrator


def run_integration(
    image_path: str,
    poni_file: str,
    output_dir: Optional[str] = None,
    npt: int = 10000,
    x_limits: Optional[Tuple[float, float]] = None,
    y_limits: Optional[Tuple[float, float]] = None,
) -> Tuple[str, str]:
    """
    Perform 1D azimuthal integration for a single image.

    Returns paths to the generated .dat and .png files.
    """
    image_path = str(image_path)
    output_root = Path(output_dir) if output_dir else Path(image_path).parent
    output_root.mkdir(parents=True, exist_ok=True)

    base_name = Path(image_path).stem
    output_dat = output_root / f"{base_name}.dat"
    output_png = output_root / f"{base_name}.png"

    ai = AzimuthalIntegrator()
    ai.load(poni_file)

    image = fabio.open(image_path).data
    two_theta, intensity = ai.integrate1d(image, npt=npt)

    np.savetxt(
        output_dat,
        np.column_stack((two_theta, intensity)),
        header="2theta Intensity",
        comments="",
    )

    plt.figure(figsize=(8, 5))
    plt.plot(two_theta, intensity, lw=2)
    plt.xlabel("2theta (deg)")
    plt.ylabel("Intensity")

    if x_limits:
        plt.xlim(*x_limits)
    if y_limits:
        plt.ylim(*y_limits)

    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    plt.close()

    return str(output_dat), str(output_png)


def integrate_directory(
    input_directory: str,
    poni_file: str,
    output_directory: Optional[str] = None,
    extensions: Iterable[str] = (".tif", ".tiff"),
    npt: int = 10000,
    x_limits: Optional[Tuple[float, float]] = None,
    y_limits: Optional[Tuple[float, float]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Perform 1D azimuthal integration on all TIFF images in a directory tree.

    Returns a mapping of image stems to their output paths.
    """
    results: Dict[str, Dict[str, str]] = {}
    for path in Path(input_directory).rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in extensions:
            continue

        dat_path, png_path = run_integration(
            image_path=str(path),
            poni_file=poni_file,
            output_dir=output_directory,
            npt=npt,
            x_limits=x_limits,
            y_limits=y_limits,
        )
        results[path.stem] = {"dat": dat_path, "png": png_path}

    return results


__all__ = [
    "run_integration",
    "integrate_directory",
]
