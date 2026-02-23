import os
import json
import tempfile
from typing import Any
from dagster import IOManager, InputContext, OutputContext, io_manager, StringSource
from girder_client import GirderClient as GC

class GirderIOManager(IOManager):
    """
    Uploads asset outputs to Girder.
    Serializes dicts to temp files, uploads, then cleans up.
    """

    def __init__(self, girder_client: GC, folder_id: str):
        self.gc = girder_client
        self.folder_id = folder_id

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Serialize and upload output to Girder folder."""
        asset_name = context.asset_key.path[-1]
        
        with tempfile.TemporaryDirectory() as tmpdir:
        # these are temporary placeholders for actual data formats
            if asset_name == "azimuthal_integration":
                # dict[scan_num] -> .dat files
                for scan_num, data in obj.items():
                    filename = f"scan_{scan_num}_azimuthal.dat"
                    filepath = os.path.join(tmpdir, filename)
                    self._write_dat(filepath, data)
                    self._upload_to_girder(filepath, filename)
                    
            elif asset_name == "lattice_parameters":
                # dict[scan_num] -> .xlsx files
                for scan_num, data in obj.items():
                    filename = f"scan_{scan_num}_lattice.xlsx"
                    filepath = os.path.join(tmpdir, filename)
                    self._write_xlsx(filepath, data)
                    self._upload_to_girder(filepath, filename)
                    
            elif asset_name == "xrf_fit":
                # dict[scan_num] -> .txt or .json files
                for scan_num, data in obj.items():
                    filename = f"scan_{scan_num}_xrf_fit.json"
                    filepath = os.path.join(tmpdir, filename)
                    self._write_json(filepath, data)
                    self._upload_to_girder(filepath, filename)
                    
            elif asset_name == "concentrations":
                # dict[scan_num] -> .xlsx files
                for scan_num, data in obj.items():
                    filename = f"scan_{scan_num}_concentrations.xlsx"
                    filepath = os.path.join(tmpdir, filename)
                    self._write_xlsx(filepath, data)
                    self._upload_to_girder(filepath, filename)
                    
            elif asset_name == "poni":
                # .poni file
                filename = "calibration.poni"
                filepath = os.path.join(tmpdir, filename)
                self._write_poni(filepath, obj)
                self._upload_to_girder(filepath, filename)
                
            elif asset_name == "mca":
                # dict[scan_num] -> .mca files
                for scan_num, data in obj.items():
                    filename = f"scan_{scan_num}.mca"
                    filepath = os.path.join(tmpdir, filename)
                    self._write_mca(filepath, data)
                    self._upload_to_girder(filepath, filename)

        context.log.info(f"Uploaded {asset_name} outputs to Girder folder {self.folder_id}")

    def _upload_to_girder(self, local_path: str, remote_name: str) -> None:
        """Upload a file to the Girder folder."""
        self.gc.uploadFileToFolder(self.folder_id, local_path, remote_name)

    def _write_dat(self, filepath: str, data: dict) -> None:
        """Serialize azimuthal integration data to .dat file."""
        # Placeholder: adjust based on your data structure
        with open(filepath, "w") as f:
            json.dump(data, f)

    def _write_xlsx(self, filepath: str, data: dict) -> None:
        """Serialize to .xlsx file (requires openpyxl or pandas)."""
        import pandas as pd
        df = pd.DataFrame([data])
        df.to_excel(filepath)

    def _write_json(self, filepath: str, data: dict) -> None:
        """Serialize to .json file."""
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

    def _write_poni(self, filepath: str, data: dict) -> None:
        """Write PyFAI PONI format file."""
        # Placeholder: adjust based on your poni dict structure
        with open(filepath, "w") as f:
            for key, value in data.items():
                f.write(f"{key} = {value}\n")

    def _write_mca(self, filepath: str, data: dict) -> None:
        """Write PyMCA .mca format file."""
        # Placeholder: adjust based on your mca dict structure
        with open(filepath, "w") as f:
            json.dump(data, f)

# Example from Dagster docs for a custom IOManager that uploads to a database API
# class DatabaseAPIIOManager(dg.ConfigurableIOManager):
#     api_token: str
#     database_url: str
    
#     def __init__(self, api_token: str, database_url: str):
#         self.api_token = api_token
#         self.database_url = database_url
#         # Initialize in-memory cache for passing data between assets
#         self._memory_cache = {}
    
#     def handle_output(self, context: dg.OutputContext, obj):
#         # Store in memory for intermediate assets
#         self._memory_cache[context.asset_key] = obj
        
#         # Only write to database for final assets (you can customize this logic)
#         if context.has_tag("final_asset"):
#             # Write to database
#             self._write_to_database(context.asset_key, obj)
    
#     def load_input(self, context: dg.InputContext):
#         # First check if data is in memory cache
#         if context.asset_key in self._memory_cache:
#             return self._memory_cache[context.asset_key]
        
#         # If not in cache, download from database API
#         return self._download_from_database(context.asset_key)
    
#     def _download_from_database(self, asset_key: dg.AssetKey):
#         # Implement your database API download logic here
#         # This is where you'd make API calls using self.api_token
#         pass
    
#     def _write_to_database(self, asset_key: dg.AssetKey, data):
#         # Implement your database write logic here
#         pass

# USAGE EXAMPLE

# @dg.asset
# def upstream_asset_1(context: dg.AssetExecutionContext):
#     # This asset's data will be downloaded from the database API
#     # when needed by downstream assets
#     return transform_data()

# @dg.asset(tags={"final_asset": "true"})
# def final_asset(intermediate_asset):
#     # This asset will be written to the database
#     # due to the "final_asset" tag
#     result = final_transformation(intermediate_asset)
#     return result

# @dg.Definitions
# def defs():
#     return dg.Definitions(
#         assets=[upstream_asset_1, upstream_asset_2, intermediate_asset, final_asset],
#         resources={
#             "io_manager": DatabaseAPIIOManager(
#                 api_token="your_api_token",
#                 database_url="your_database_url"
#             )
#         }
#     )