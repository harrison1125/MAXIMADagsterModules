from dagster import asset, AssetExecutionContext, Config
import h5py
import re
import os
import tempfile

from .modules import *
from .resources import GirderClient
from girder_client import HttpError

# upstream assets
class RawDataConfig(Config):
    folder_id: str

@asset(required_resource_keys={"GirderClient"})
def xrdxrf_scans(context: AssetExecutionContext, config: RawDataConfig):
    # fetches the raw data off Girder and loads it into a dictionary of shape:
    # {
    #   scan_num: {
    #     "xrd": np.ndarray,
    #     "xrf": str,
    #     "source_files": [str, str, ...]
    #   },
    #   ...
    # }

    gc = context.resources.GirderClient

    h5_pattern = re.compile(r"^scan_point_(\d+)_data_\d+\.h5$")
    xrf_pattern = re.compile(r"^scan_point_(\d+)\.xrf$")

    scans: dict[int, dict] = {}

    items = gc.listFolder(config.folder_id)
    for item in items:
        files = list(gc.listItem(item["_id"]))
        for f in files:
            name = f["name"]
            h5_match = h5_pattern.match(name)
            xrf_match = xrf_pattern.match(name)
            if not h5_match and not xrf_match:
                continue

            scan_num = int((h5_match or xrf_match).group(1))
            scans.setdefault(scan_num, {})

            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, name)
                gc.downloadFile(f["_id"], local_path)

                if h5_match:
                    with h5py.File(local_path, "r") as h5f:
                        scans[scan_num]["xrd"] = h5f["entry"]["data"][:]
                else:
                    with open(local_path, "r", encoding="utf-8", errors="replace") as xrf:
                        scans[scan_num]["xrf"] = xrf.read()

            scans[scan_num].setdefault("source_files", []).append(name)

    context.log.info(f"Loaded {len(scans)} scan(s) from folder {config.folder_id}")
    return scans
    
class ModelConfig(Config):
    file_id: str

@asset(required_resource_keys={"GirderClient"})
def calibration_model(context: AssetExecutionContext, config: ModelConfig):
    gc = context.resources.GirderClient

    file_info = None
    meta = {}
    try:
        file_info = gc.getFile(config.file_id)
        file_name = file_info["name"]
        meta = file_info.get("meta") or {}
    except HttpError:
        item_info = gc.getItem(config.file_id)
        meta = item_info.get("meta") or {}
        item_files = list(gc.listFile(item_info["_id"]))
        pth_files = [f for f in item_files if f.get("name", "").lower().endswith(".pth")]
        if not pth_files:
            raise ValueError("No .pth files found for the provided item id.")
        file_info = pth_files[0]
        file_name = file_info["name"]

    if not file_name.lower().endswith(".pth"):
        raise ValueError(f"Expected a .pth model file, got {file_name}")

    meta_fields = meta.get("params") if isinstance(meta.get("params"), dict) else meta
   
    required_fields = ("calibrant", "detector", "energy", "version")
    missing = [field for field in required_fields if field not in meta_fields or meta_fields[field] in (None, "")]
    if missing:
        raise ValueError(
            f"Girder file metadata is missing required field(s): {missing}. "
                f"Found metadata keys: {sorted(meta_fields.keys())}"
        )

    calibrant = str(meta_fields["calibrant"])
    detector = str(meta_fields["detector"])
    energy = float(meta_fields["energy"])
    version = str(meta_fields["version"])

    local_dir = os.path.join("data", "models")
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, file_name)

    if not os.path.exists(local_path):
        gc.downloadFile(file_info["_id"], local_path)
        context.log.info(f"Downloaded calibration model to {local_path}")
    else:
        context.log.info(f"Using cached calibration model at {local_path}")

    model = calibrate.MaximaCalibrator(local_path, calibrant, detector, energy)

    return {
        "model": model,
        "metadata": {
            "source_file": file_name,
            "calibrant": calibrant,
            "detector": detector,
            "energy": energy,
            "version": version,
        },
    }

class PymcaConfig(Config):
    file_id: str  # Girder file _id for the pymca config file

@asset(required_resource_keys={"GirderClient"})
def pymca_config(context: AssetExecutionContext, config: PymcaConfig):
    gc = context.resources.GirderClient

    file_info = gc.getFile(config.file_id)
    file_name = file_info["name"]

    if not file_name.lower().endswith(".cfg"):
        raise ValueError(f"Expected a .cfg file, got {file_name}")

    local_dir = os.path.join("data", "configs")
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, file_name)

    if not os.path.exists(local_path):
        gc.downloadFile(config.file_id, local_path)
        context.log.info(f"Downloaded PyMCA config to {local_path}")
    else:
        context.log.info(f"Using cached PyMCA config at {local_path}")

    return local_path

# xrd processing assets
@asset
def poni(xrdxrf_scans, calibration_model):
    # generates .poni file for the dataset using the calibration model and metadata from the scans.
    # assumes scan_point_0 is a calibration scan NOTE: ask about what we want the actual practice to be here. Do we want to enforce a specific scan to be the calibration, or should we be more flexible and look for metadata in the xrf or something else?
    scan0 = xrdxrf_scans.get(0)
    if not scan0:
        raise ValueError("Scan point 0 is required for calibration but was not found.")
    
    xrd_data = scan0.get("xrd")
    if xrd_data is None:
        raise ValueError("Scan point 0 does not contain XRD data required for calibration.")
    
    model = calibration_model["model"]

    return model.calibrate(xrd_data) # returns pyFAI geometry object

@asset
def azimuthal_integration(xrdxrf_scans, poni):
    # performs azimuthal integration on all scans using the generated geometry and returns scan->DataFrame
    xrd_scans = {num: scan["xrd"] for num, scan in xrdxrf_scans.items() if "xrd" in scan}
    
    return AzimuthalIntegrator.integrate_dict(xrd_scans, poni)

@asset
def lattice_parameters(azimuthal_integration):
    # returns per-scan lattice parameter DataFrames from in-memory integrated scans
    return LatticeParameters.process_integrated_dict(azimuthal_integration)

# xrf processing assets
@asset 
def mca(xrdxrf_scans):
    # converts the raw xrf data for each scan to mca format
    xrf_scans = {num: scan["xrf"] for num, scan in xrdxrf_scans.items() if "xrf" in scan}

    return ConverterXRFtoMCA.convert_xrf_to_mca(xrf_scans) # this module needs to be rewritten to take in the appropriate data formats for dagster assets and return a mapping of scan numbers to their converted mca data

@asset
def xrf_fit(mca, pymca_config):
    # performs peak fitting on the mca data using the provided pymca config file. Returns a mapping of scan numbers to their fit results
    return MCAtoFit.run_pymca_batch(mca, pymca_config) # this module also needs to be rewritten to take in the appropriate data formats for dagster assets and return a mapping of scan numbers to their fit results and plots

@asset
def concentrations(xrf_fit):
    # calculates concentrations from the xrf fit results. Returns a mapping of scan numbers to their calculated concentrations
    return concentrations.process_pymca_directory(xrf_fit) # this module also needs to be rewritten to take in the appropriate data formats for dagster assets and return a mapping of scan numbers to their calculated concentrations

# push to girder
class PushToGirderConfig(Config):
    folder_id: str  # Girder folder _id where results should be uploaded  
