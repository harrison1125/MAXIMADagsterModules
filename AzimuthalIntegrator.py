import os
import fabio
import numpy as np
import matplotlib.pyplot as plt
# from dagster import asset
from pyFAI.integrator.azimuthal import AzimuthalIntegrator


import Inputs
# import ConverterXRFtoMCA  # currently unused

# @asset
def azimuthally_integrate_files(
    input_directory: str,
    poni_file: str,
    azimuth_range=(30, 40),
    npt_rad: int = 100,
    npt_azim: int = 100,
    #Should be dynamic eventually
    y_limits=(0, 1.8),
    x_limits=(39.75, 41.25),
) -> None:
    """
    Perform azimuthal integration on all TIFF images in a directory tree.

    This function walks through `input_directory`, identifies `.tif` and `.tiff`
    files, performs 2D azimuthal integration using a single pyFAI `.poni`
    calibration file, and saves both numerical results and diagnostic plots.

    Parameters
    ----------
    input_directory : str
        Root directory containing TIFF images to be processed.
    poni_file : str
        Path to the pyFAI calibration (.poni) file.
    azimuth_range : tuple of float, optional
        Azimuthal angle range (degrees) used for integration.
    npt_rad : int, optional
        Number of radial bins for integration.
    npt_azim : int, optional
        Number of azimuthal bins for integration.
    y_limits : tuple of float, optional
        Y-axis limits for output plots.
    x_limits : tuple of float, optional
        X-axis limits for output plots.

    Returns
    -------
    Array as asset
        Results are written to disk as `.dat` and `.png` files.
    """

    # Initialize azimuthal integrator once
    ai = AzimuthalIntegrator()
    ai.load(poni_file)
    results = {}

    for dirpath, _, filenames in os.walk(input_directory):
        for filename in filenames:
            base_name, ext = os.path.splitext(filename)
            ext = ext.lower()

            if ext not in (".tif", ".tiff"):
                continue

            input_path = os.path.join(dirpath, filename)
            output_dat = os.path.join(dirpath, f"{base_name}.dat")
            output_png = os.path.join(dirpath, f"{base_name}.png")

            try:
                image = fabio.open(input_path).data

                # pyFAI integrate2d returns:
                # I (2D), radial axis, azimuthal axis
                two_theta, intensity = ai.integrate1d(image, npt = 10000)
                # intensity, radial, azimuth = ai.integrate2d(
                #    image,
                #    azimuth_range=azimuth_range,
                #    npt_rad=npt_rad,
                #    npt_azim=npt_azim,
                #)

                # Save numerical output
                np.savetxt(
                    output_dat,
                    np.column_stack((two_theta, intensity)),
                    header="2theta Intensity",
                    comments="",
                )

                # Work in progress: some leftover issues from trying to form integrator2d

                # Plot azimuthal dependence (example slice)
                plt.figure(figsize=(8, 5))
                plt.plot(azimuth, intensity.mean(axis=0), lw=2)
                plt.xlabel(r"$Q$ (nm$^{-1}$)", fontsize=15)
                plt.ylabel("Intensity", fontsize=15)

                plt.xlim(*x_limits)
                plt.ylim(*y_limits)

                plt.tick_params(axis="both", which="major", labelsize=15)
                plt.tight_layout()
                plt.savefig(output_png, dpi=150)
                plt.close()

                print(f"Saved: {output_dat}, {output_png}")
                
                results[base_name] = {
                    "Q": azimuth,
                    "intensity": intensity.mean(axis=0),
                }
            except Exception as exc:
                print(f"Failed to process {input_path}: {exc}")
        
if __name__ == "__main__":
    azimuthally_integrate_files(
        input_directory=Inputs.root_dir,
        poni_file=Inputs.poni_file,
              )
