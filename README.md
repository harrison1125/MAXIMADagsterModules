# MAXIMA Dagster Modules

Dagster project for sensor-driven XRD/XRF experiment processing against Girder-backed data from MAXIMA.
It detects new experiments, materializes partitioned assets, caches model/calibration artifacts,
and publishes processed XRD outputs back to Girder.

![alt text](maxima-dagster.png)

## What It Does

- Watches a Girder root folder for new experiment folders that contain `raw/scan_point_*_data_*.h5` files.
- Triggers one Dagster run per new experiment using dynamic partitions.
- Builds an XRD pipeline that includes scan loading, calibration model use, PONI generation, azimuthal integration, and lattice parameter extraction.
- Publishes result artifacts (including run manifest + CSV outputs) back to the experiment folder in Girder.
- Caches heavy artifacts locally to reduce repeated compute:
  - model files in `data/models`
  - PONI files and index metadata in `data/calibrations`

## Current Pipeline Scope

- XRD sensor-driven flow is operational.
- XRF-related assets exist in the codebase but are not part of the primary sensor-driven `xrd_test_job` path yet.

## Core Components

- `experiment_folder_sensor`: detects new experiments and launches `xrd_test_job` with partition key = Girder experiment folder id.
- `calibration_scan_sensor`: detects latest calibrant scan updates and launches `calibration_precompute_job`.
- `xrd_test_job`: materializes XRD assets for a partitioned experiment.
- `calibration_precompute_job`: precomputes and refreshes calibration prerequisites.

## Repository Layout

- `src/MaximaDagster/`: Dagster definitions, assets, sensors, resources, and modules.
- `tests/`: pytest coverage for assets, modules, and sensor behavior.
- `data/`: local cache for models and calibration outputs.
- `dagster_home/`: Dagster instance configuration and run/storage artifacts.
- `docker-compose.yml` and `Dockerfile`: containerized runtime.

## Prerequisites

- Python `>=3.10,<3.15` (Python 3.11 recommended)
- Access to the target Girder instance and folder IDs
- Optional: Docker + Docker Compose for containerized deployment

## Required Environment Variables

Copy `.env.example` to `.env` and provide values:

- `GIRDER_API_URL`: Girder API base URL
- `GIRDER_API_KEY`: API key for an account with read/write access to relevant folders
- `GIRDER_ROOT_FOLDER_ID`: parent folder containing experiment subfolders
- `GIRDER_CALIBRANTS_FOLDER_ID`: folder containing calibrant `.h5` scan files
- `GIRDER_MODEL_ITEM_OR_FILE_ID`: Girder item or file ID for the `.pth` calibration model

## Local Development

### 1. Create and activate an environment

Example (Conda):

```powershell
conda create -n dagster python=3.11 -y
conda activate dagster
```

### 2. Install dependencies

```powershell
pip install -e .
pip install dagster-webserver dagster-dg-cli pytest
```

### 3. Configure environment variables

On PowerShell:

```powershell
Copy-Item .env.example .env
# Edit .env with your Girder values
```

Set `DAGSTER_HOME` for the current shell:

```powershell
$env:DAGSTER_HOME = (Resolve-Path .\dagster_home)
```

### 4. Run Dagster locally

```powershell
dagster dev -w workspace.yaml
```

Open the UI at `http://localhost:3000`.

Enable the sensors in the "Automation" tab to start monitoring for new experiments and calibrant updates.

## Docker Quickstart

1. Create and edit `.env`:

```powershell
Copy-Item .env.example .env
```

2. Build and start services:

```powershell
docker compose up --build -d
```

3. View the Dagster UI:

- `http://localhost:3000`

4. Tail logs for sensors and webserver:

```powershell
docker compose logs -f dagster-daemon dagster-webserver
```

5. Stop services:

```powershell
docker compose down
```

## Operational Notes

- Tests can be run with pytest
- Experiment partition keys are Girder experiment folder IDs
- Caching reduces repeated downloads/recalibration but can be invalidated by deleting local cache files under `data/models` and `data/calibrations`.
- `dagster_home/` contains local Dagster state

## Troubleshooting

- No sensor runs: verify all required `GIRDER_*` variables are set and valid.
- Runs fail to load assets: confirm `workspace.yaml` resolves `MaximaDagster.definitions` in your environment.
- Calibration not refreshing: ensure new calibrant scans match the expected filename pattern `xrd_calibrant_data_<id>.h5` (based on available examples, probably needs to adjusted)