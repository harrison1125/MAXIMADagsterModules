Configuration
=============

Environment Variables
---------------------

Core runtime:

- GIRDER_API_URL: Girder API base URL
- GIRDER_API_KEY: API key for read/write operations
- GIRDER_MODEL_ITEM_ID: item ID that contains the calibration model file

Discovery backend controls:

- DISCOVERY_DATAFILES_RETRY_COUNT: retry attempts for datafiles calls
- DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS: delay between retries

Dagster Home
------------

Set DAGSTER_HOME to the repository dagster_home directory when running locally.
