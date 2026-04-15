Configuration
=============

Environment Variables
---------------------

Core runtime:

- GIRDER_API_URL: Girder API base URL
- GIRDER_API_KEY: API key for read/write operations
- GIRDER_ROOT_FOLDER_ID: parent folder containing experiment subfolders
- GIRDER_CALIBRANTS_FOLDER_ID: folder containing calibrant scan files
- GIRDER_MODEL_ITEM_ID: item ID that contains the calibration model file

Discovery backend controls:

- DISCOVERY_BACKEND: legacy or datafiles
- DISCOVERY_ALLOW_FALLBACK: allow fallback from datafiles to legacy traversal
- DISCOVERY_DATAFILES_LIMIT: page size for datafiles API pulls
- DISCOVERY_DATAFILES_MAX_PAGES: max pages for datafiles queries
- DISCOVERY_DATAFILES_MAX_ROWS: max rows retained from datafiles pulls
- DISCOVERY_DATAFILES_RETRY_COUNT: retry attempts for datafiles calls
- DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS: delay between retries

Dagster Home
------------

Set DAGSTER_HOME to the repository dagster_home directory when running locally.
