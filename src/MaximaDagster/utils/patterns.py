"""Centralized filename pattern definitions for XRD scan files."""

import re

# XRD scan H5 files: scan_point_<id>_data_<counter>.h5
H5_SCAN_PATTERN = re.compile(r"^scan_point_(\d+)_data_\d+\.h5$", re.IGNORECASE)

# Calibrant XRD scan H5 files: xrd_calibrant_data_<id>.h5
CALIBRANT_SCAN_PATTERN = re.compile(r"^xrd_calibrant_data_(\d+)\.h5$", re.IGNORECASE)

__all__ = [
    "H5_SCAN_PATTERN",
    "CALIBRANT_SCAN_PATTERN",
]
