# energy
Python app to query energy data from the NREL ResStock dataset. 

1. Add .parquet file to /energydata folder (https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2021%2Fresstock_amy2018_release_1%2Fmetadata%2F)

2. Run on Linux/WSL: gunicorn -k eventlet -w 1 --reload energy:app