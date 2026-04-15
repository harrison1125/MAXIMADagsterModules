Dagster Runtime
===============

Jobs
----

- xrd: partitioned experiment processing flow
- calibration_precompute: calibrant-driven precompute flow
- discovery_smoke: diagnostic smoke check for discovery operations

Sensors
-------

- experiment_folder_sensor
  - Polls for experiments with raw XRD scan files
  - Adds dynamic partitions and launches xrd runs
- calibration_scan_sensor
  - Polls calibrant scans
  - Launches calibration_precompute runs when new calibrant input appears

Asset Graph
-----------

The runtime critical assets include:

- xrdxrf_scans (XRD scan loading payload)
- calibration_model
- poni
- azimuthal_integration
- publish_xrd_results

Note: lattice_parameters exists as an asset implementation, but is not currently selected in the xrd job definition.
