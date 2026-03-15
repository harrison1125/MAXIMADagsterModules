from dagster import Definitions, define_asset_job, fs_io_manager
from .resources import GirderClient
from .assets import *
from .sensors import calibration_scan_sensor, experiment_folder_sensor, experiment_partitions

xrd_test_job = define_asset_job(
    name="xrd_test_job",
    selection=["xrdxrf_scans", "calibration_model", "poni", "azimuthal_integration", "lattice_parameters", "publish_xrd_results"],
)

calibration_precompute_job = define_asset_job(
    name="calibration_precompute_job",
    selection=["calibration_model", "poni"],
)

defs = Definitions(
    assets=[xrdxrf_scans, calibration_model, pymca_config, poni, azimuthal_integration, lattice_parameters, publish_xrd_results, mca, xrf_fit, concentrations],
    jobs=[xrd_test_job, calibration_precompute_job],
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