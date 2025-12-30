"""
Input paths for XRD and XRF analysis workflows

This module centralizes the filepaths used in automated XRD analysis workflows for further use. This discretizes input values from further analysis scripts, which increases cleanliness down the line as we separate manual vs automated workflows (manual workflows use this input folder, automated workflows naturally stream new results without requiring explicitly defining filepaths). 

Attributes
----------
root_dir : str
    Path to directory containing all files to analyze.
poni_file : str
    Path to calibration file for XRD. Generated through PyFAI.
config_path : str
    Path to configuration file for XRF. Generated through PYMCA.
"""
root_dir = '/Users/hpark108/Desktop/Immediate/20250930 updated XRF CuTi Samples for Rohit'
poni_file = '/Users/hpark108/Desktop/Immediate/updated xrf.poni'
config_path = '/Users/hpark108/Desktop/Immediate/aimdpaper.cfg'