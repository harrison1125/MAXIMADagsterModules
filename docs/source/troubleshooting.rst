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

GitHub Pages 404
----------------

If GitHub Actions succeeds but the site is 404:

- Verify a Pages deploy workflow exists and is enabled.
- Verify deployment artifact path is docs/build/html.
- Verify the branch and environment are configured for Pages.
- Verify html_baseurl matches the repository Pages URL.
- Rebuild docs from a clean docs/build directory before deployment.
