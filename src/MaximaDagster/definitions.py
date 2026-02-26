from dagster import Definitions, define_asset_job, fs_io_manager, EnvVar
from .resources import GirderClient
from .io_manager import ConfigurableGirderIOManager
from .assets import *
from .sensors import experiment_folder_sensor, experiment_partitions

xrd_test_job = define_asset_job(
    name="xrd_test_job",
    selection=["xrdxrf_scans", "calibration_model", "poni", "azimuthal_integration", "lattice_parameters", "publish_xrd_results"],
)

defs = Definitions(
    assets=[xrdxrf_scans, calibration_model, pymca_config, poni, azimuthal_integration, lattice_parameters, publish_xrd_results, mca, xrf_fit, concentrations],
    jobs=[xrd_test_job],
    sensors=[experiment_folder_sensor],
    resources={
        "GirderClient": GirderClient.configured(
            {
                "api_url": {"env": "GIRDER_API_URL"},
                "api_key": {"env": "GIRDER_API_KEY"},
            }
        ),
        "io_manager": fs_io_manager,
        "girder_io_manager": ConfigurableGirderIOManager(
            api_url=EnvVar("GIRDER_API_URL"),
            api_key=EnvVar("GIRDER_API_KEY"),
            source_folder_id=EnvVar("GIRDER_SOURCE_FOLDER_ID"),
            target_folder_id=EnvVar("GIRDER_TARGET_FOLDER_ID"),
        ),
    },
)