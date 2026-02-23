from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

import fabio
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyFAI.geometry import Geometry
from pyFAI.integrator.azimuthal import AzimuthalIntegrator as PyFAIAzimuthalIntegrator


Q_COLUMN = "q_nm^-1"
INTENSITY_COLUMN = "intensity"


def _coerce_pattern(image: np.ndarray) -> np.ndarray:
    pattern = np.asarray(image)
    if pattern.ndim != 2:
        raise ValueError(f"Expected a 2D diffraction image, got ndim={pattern.ndim}")
    if pattern.size == 0:
        raise ValueError("Expected a non-empty diffraction image")
    if not np.issubdtype(pattern.dtype, np.number):
        raise ValueError("Expected a numeric diffraction image array")
    return pattern.astype(np.float32, copy=False)


def _create_integrator_from_geometry(geometry: Geometry) -> PyFAIAzimuthalIntegrator:
    ai = PyFAIAzimuthalIntegrator(
        dist=geometry.dist,
        poni1=geometry.poni1,
        poni2=geometry.poni2,
        rot1=geometry.rot1,
        rot2=geometry.rot2,
        rot3=geometry.rot3,
        detector=geometry.detector,
        wavelength=geometry.wavelength,
    )
    return ai


def _create_integrator_from_poni(poni_file: str) -> PyFAIAzimuthalIntegrator:
    ai = PyFAIAzimuthalIntegrator()
    ai.load(poni_file)
    return ai


def _integrate_with_ai(
    ai: PyFAIAzimuthalIntegrator,
    image: np.ndarray,
    npt: int = 10000,
    radial_range: Optional[Tuple[float, float]] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    kwargs: Dict[str, Any] = {"npt": npt, "unit": Q_COLUMN}
    if radial_range is not None:
        kwargs["radial_range"] = radial_range

    pattern = _coerce_pattern(image)
    try:
        q_vals, intensity = ai.integrate1d(pattern, **kwargs)
    except TypeError:
        kwargs.pop("unit", None)
        q_vals, intensity = ai.integrate1d(pattern, **kwargs)

    return np.asarray(q_vals), np.asarray(intensity)


def integrate_pattern(
    image: np.ndarray,
    ai: PyFAIAzimuthalIntegrator,
    npt: int = 10000,
    radial_range: Optional[Tuple[float, float]] = None,
) -> pd.DataFrame:
    """
    Perform in-memory 1D azimuthal integration for a single diffraction pattern.

    Parameters
    ----------
    image : np.ndarray
        2D diffraction pattern.
    ai : PyFAIAzimuthalIntegrator
        Configured pyFAI integrator instance.
    npt : int, optional
        Number of radial points.
    radial_range : tuple[float, float], optional
        Q range to integrate over.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns `q_nm^-1` and `intensity`.
    """
    q_vals, intensity = _integrate_with_ai(ai, image=image, npt=npt, radial_range=radial_range)
    return pd.DataFrame({Q_COLUMN: q_vals, INTENSITY_COLUMN: intensity})


def integrate_dict(
    xrd_scans: Mapping[int, np.ndarray],
    geometry: Geometry,
    npt: int = 10000,
    radial_range: Optional[Tuple[float, float]] = None,
) -> Dict[int, pd.DataFrame]:
    """
    Perform in-memory 1D azimuthal integration for a mapping of scan IDs to images.

    Parameters
    ----------
    xrd_scans : Mapping[int, np.ndarray]
        Mapping of scan ID to 2D diffraction images.
    geometry : Geometry
        pyFAI Geometry object for integration.
    npt : int, optional
        Number of radial points.
    radial_range : tuple[float, float], optional
        Q range to integrate over.

    Returns
    -------
    Dict[int, pd.DataFrame]
        Mapping of scan ID to integrated pattern DataFrame with columns
        `q_nm^-1` and `intensity`.
    """
    ai = _create_integrator_from_geometry(geometry)
    outputs: Dict[int, pd.DataFrame] = {}
    for scan_id, image in xrd_scans.items():
        outputs[int(scan_id)] = integrate_pattern(
            image=image,
            ai=ai,
            npt=npt,
            radial_range=radial_range,
        )
    return outputs


def run_integration(
    image_path: str,
    poni_file: Union[str, Geometry],
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

    if isinstance(poni_file, Geometry):
        ai = _create_integrator_from_geometry(poni_file)
    else:
        ai = _create_integrator_from_poni(str(poni_file))

    image = fabio.open(image_path).data
    q_vals, intensity = _integrate_with_ai(ai, image=image, npt=npt)

    np.savetxt(
        output_dat,
        np.column_stack((q_vals, intensity)),
        header="Q_nm^-1 Intensity",
        comments="",
    )

    plt.figure(figsize=(8, 5))
    plt.plot(q_vals, intensity, lw=2)
    plt.xlabel("Q (1/nm)")
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
    poni_file: Union[str, Geometry],
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
    "Q_COLUMN",
    "INTENSITY_COLUMN",
    "integrate_pattern",
    "integrate_dict",
    "run_integration",
    "integrate_directory",
]
