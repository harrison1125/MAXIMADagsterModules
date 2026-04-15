Troubleshooting
===============

No Sensor Runs
--------------

- Verify GIRDER_* environment variables are set and valid.
- Confirm sensors are enabled in Dagster Automation.

Asset Import or Load Errors
---------------------------

- Confirm workspace.yaml resolves MaximaDagster.definitions.
- Confirm package install is active in the selected environment.

Calibration Not Refreshing
--------------------------

- Validate calibrant filenames against the expected calibrant pattern.
- Verify calibration_scan_sensor cursor movement and fallback behavior.