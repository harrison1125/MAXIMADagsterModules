from dagster import Definitions, define_asset_job, fs_io_manager
from .resources import GirderClient
from .assets import *
from .sensors import xrd_experiment_sensor, xrd_calibration_sensor


xrd = define_asset_job(
    name="xrd",
    selection=["xrd_raw", "active_poni", "azimuthal_integration"],
)

calibration_precompute = define_asset_job(
    name="calibration_precompute",
    selection=["calibration_model", "xrd_calibrant_raw", "poni"],
)

defs = Definitions(
    assets=[xrd_raw, xrd_calibrant_raw, calibration_model, poni, azimuthal_integration, active_poni],
    jobs=[xrd, calibration_precompute],
    sensors=[xrd_experiment_sensor, xrd_calibration_sensor],
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