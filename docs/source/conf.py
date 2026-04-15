from pathlib import Path
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'MAXIMA Dagster Modules'
copyright = '2025, Hyun Sang Park'
author = 'Hyun Sang Park'

with (ROOT / "pyproject.toml").open("rb") as pyproject_file:
    _project_data = tomllib.load(pyproject_file)

release = str(_project_data.get("project", {}).get("version", "unknown"))
version = release

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

autosummary_generate = True
autodoc_typehints = "description"
autodoc_member_order = "bysource"

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_baseurl = "https://gmurra12.github.io/MAXIMADagsterModules/"

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

