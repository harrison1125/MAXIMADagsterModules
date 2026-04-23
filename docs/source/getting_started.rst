Getting Started
===============

This project runs a sensor-driven XRD processing workflow on Dagster.

Prerequisites
-------------

- Python 3.11 to 3.14
- Access to the target Girder instance and folder IDs
- Optional: Docker and Docker Compose

Install
-------

.. code-block:: powershell

   pip install -e .
   pip install dagster-webserver dagster-dg-cli pytest sphinx sphinx-rtd-theme

Set Environment
---------------

Required variables:

- GIRDER_API_URL
- GIRDER_API_KEY
- GIRDER_ROOT_FOLDER_ID
- GIRDER_CALIBRANTS_FOLDER_ID
- GIRDER_MODEL_ITEM_ID

Optional discovery tuning variables:

- DISCOVERY_DATAFILES_RETRY_COUNT
- DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS

Run Locally
-----------

.. code-block:: powershell

   $env:DAGSTER_HOME = (Resolve-Path .\dagster_home)
   dagster dev -w workspace.yaml

Open http://localhost:3000 and enable sensors in Automation.

Build Docs
----------

.. code-block:: powershell

   Remove-Item -Recurse -Force .\docs\build
   sphinx-build -b html docs/source docs/build/html
