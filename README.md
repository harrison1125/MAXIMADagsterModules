# Dagster for AIMD-L MAXIMA

## XRDXRF Dagster Workflow

The Dagster deployment now supports a sensor-driven experiment pipeline for XRD assets. XRF still WIP. 

### Required environment variables

- `GIRDER_API_URL`
- `GIRDER_API_KEY`
- `GIRDER_ROOT_FOLDER_ID` (folder containing experiment subfolders)
- `GIRDER_CALIBRANTS_FOLDER_ID` (folder containing calibration scan `.h5` files)
- `GIRDER_MODEL_ITEM_OR_FILE_ID` (Girder item/file id of the calibration model `.pth`)

### Behavior

- `experiment_folder_sensor` scans `GIRDER_ROOT_FOLDER_ID` and looks for experiment folders containing `raw/` with `scan_point_*_data_*.h5` files
- Each new experiment folder triggers one run of `xrd_test_job` using a dynamic partition whose key is the experiment folder id
- `xrdxrf_scans` loads scan payloads from `root/<experiment>/raw`
- `calibration_model` uses cached model weights in `data/models` and downloads from Girder only on cache miss.
- `poni` checks `GIRDER_CALIBRANTS_FOLDER_ID` for the newest calibrant scan and caches generated `.poni` files in `data/calibrations` keyed by calibrant scan file id
- `publish_xrd_results` uploads `.poni`, azimuthal `.csv`, lattice `.csv`, and `xrd_run_manifest.json` into the experiment folder in Girder

