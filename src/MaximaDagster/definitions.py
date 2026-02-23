from dagster import Definitions, define_asset_job, fs_io_manager
from .resources import GirderClient
from .assets import *

xrd_test_job = define_asset_job(
    name="xrd_test_job",
    selection=["xrdxrf_scans", "calibration_model", "poni", "azimuthal_integration", "lattice_parameters"],
)

defs = Definitions(
    assets=[xrdxrf_scans, calibration_model, pymca_config, poni, azimuthal_integration, lattice_parameters, mca, xrf_fit, concentrations],
    jobs=[xrd_test_job],
    resources={
        "GirderClient": GirderClient,
        "io_manager": fs_io_manager,
    },
)