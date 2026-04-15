Discovery Backend
=================

Backend Modes
-------------

- legacy: traverses Girder folders/items/files directly
- datafiles: queries aimdl/datafiles endpoints

Fallback Behavior
-----------------

When DISCOVERY_BACKEND is datafiles and DISCOVERY_ALLOW_FALLBACK is enabled,
runtime discovery calls can fall back to legacy traversal if a datafiles call fails.

Runtime Discovery Operations
----------------------------

- Experiment discovery for sensor launches
- Calibrant discovery for calibration precompute and PONI generation
- XRD scan candidate listing for partitioned experiment loading

Operational Notes
-----------------

Use tighter max rows and max pages values to constrain memory and API load for large repositories.
