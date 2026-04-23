Discovery Backend
=================

Runtime APIs
------------

- ``GET aimdl/partition``: sensor polling for partition checksum updates
- ``GET aimdl/partition/details``: partition detail expansion used by partitioned assets
- ``GET aimdl/datafiles``: calibrant selection helper for choosing a calibrant prior to an experiment date

Runtime Discovery Operations
----------------------------

- Experiment discovery for sensor launches (``xrd_raw``)
- Calibrant discovery for calibration precompute launches (``xrd_calibrant_raw``)
- Partition detail retrieval for ``xrd_raw`` and ``xrd_calibrant_raw`` assets

Operational Notes
-----------------

Discovery retries are controlled by:

- ``DISCOVERY_DATAFILES_RETRY_COUNT``
- ``DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS``
