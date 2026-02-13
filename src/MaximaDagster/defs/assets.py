from dagster import asset, Output, AssetIn
from ..modules import *
from ..resources import GirderClient

# upstream assets
@asset
def maxima_data_h5(girder: GirderClient):
    # pulls the raw data off Girder
    local_path = "data/raw/instrument.h5"
    girder.download_file("file_id_from_config", local_path)
    return local_path

@asset
def pymca_config():
    # assuming local config
    return "configs/pymca.cfg"

# processing modules
@asset
def mca_file(maxima_data_h5, pymca_config):
    output_path = "data/processed/sample.mca"
    ConverterXRFtoMCA.convert_xrf_to_mca(
        h5_path=maxima_data_h5, 
        cfg_path=pymca_config,
        output=output_path
    )
    return output_path

@asset
def integrated_data(poni_file): 
    dat_path, png_path = AzimuthalIntegrator.run_integration(poni_file)
    return {
        "dat": dat_path,
        "png": png_path
    }

# push to girder
@asset
def upload_lattice_results(lattice_params, girder: GirderClient):
    girder.upload_file(lattice_params["xlsx"])
    girder.upload_file(lattice_params["png"])