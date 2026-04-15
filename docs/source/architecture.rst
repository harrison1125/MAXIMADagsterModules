Architecture
============

Scope
-----

This repository is scoped to XRD processing only. XRF processing is out of scope.

High-Level Flow
---------------

1. Sensor detects new experiment folders with raw XRD scans.
2. Dagster run materializes partitioned XRD assets for that experiment.
3. Calibration model and PONI calibration are resolved and cached.
4. Azimuthal integration outputs are generated.
5. Results and provenance metadata are published back to Girder.

Runtime Components
------------------

- Definitions and jobs: MaximaDagster.definitions
- Sensors: MaximaDagster.sensors
- Assets: MaximaDagster.assets
- Processing modules: MaximaDagster.modules.*
- Shared runtime helpers: MaximaDagster.utils.*
