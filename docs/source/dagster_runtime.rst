Dagster Runtime
===============

Jobs
----

- xrd: partitioned experiment processing flow
- calibration_precompute: calibrant-driven precompute flow

Sensors
-------

- xrd_experiment_sensor
  - Polls partition checksums for ``xrd_raw``
  - Adds dynamic partitions and launches xrd runs
- xrd_calibration_sensor
  - Polls partition checksums for ``xrd_calibrant_raw``
  - Launches calibration_precompute runs when new calibrant input appears

Asset Graph
-----------

The runtime critical assets include:

- xrd_raw
- xrd_calibrant_raw
- calibration_model
- poni
- active_poni
- azimuthal_integration
