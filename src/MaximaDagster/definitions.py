from dagster import Definitions, define_asset_job, fs_io_manager, In, Out, Nothing, job, op
from .resources import GirderClient
from .assets import *
from .sensors import calibration_scan_sensor, experiment_folder_sensor, experiment_partitions
from .utils.discovery import discovery_calibrants_check, discovery_experiments_check


xrd = define_asset_job(
    name="xrd",
    selection=["xrdxrf_scans", "calibration_model", "poni", "azimuthal_integration", "publish_xrd_results"],
)

xrf = define_asset_job(
    name="xrf",
    selection=["xrdxrf_scans", "pymca_config", "mca", "xrf_fit", "concentrations"],
)

xrdxrf = define_asset_job(
    name="xrdxrf",
    selection=["xrdxrf_scans", "calibration_model", "poni", "azimuthal_integration", "publish_xrd_results", "pymca_config", "mca", "xrf_fit", "concentrations"],
)

calibration_precompute = define_asset_job(
    name="calibration_precompute",
    selection=["calibration_model", "poni"],
)

@job(name="discovery_smoke")
def discovery_smoke():
    discovery_experiments_check()
    discovery_calibrants_check()


defs = Definitions(
    assets=[xrdxrf_scans, calibration_model, pymca_config, poni, azimuthal_integration, publish_xrd_results, mca, xrf_fit, concentrations],
    jobs=[xrd, xrf, xrdxrf, calibration_precompute, discovery_smoke],
    sensors=[experiment_folder_sensor, calibration_scan_sensor],
    resources={
        "GirderClient": GirderClient.configured(
            {
                "api_url": {"env": "GIRDER_API_URL"},
                "api_key": {"env": "GIRDER_API_KEY"},
            }
        ),
        "io_manager": fs_io_manager,
    },
)