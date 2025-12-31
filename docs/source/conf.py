import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'MAXIMA_DAG'
copyright = '2025, Hyun Sang Park'
author = 'Hyun Sang Park'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',    # Pulls docstrings from Python code
    'sphinx.ext.napoleon',   # Supports Google/NumPy style docstrings
]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_baseurl = "https://hpark108.github.io/MAXIMA_DAG/"

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

