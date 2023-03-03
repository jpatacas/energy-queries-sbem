# Python apps to query energy data from the NREL ComStock and ResStock datasets.

# energy.py
socket.io app to be used with the [SBEM IFC.js](https://github.com/jpatacas/sbem-ifcjs) app to provide data for the energy modelling panel (ResStock only).

1. Add [.parquet file](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2021%2Fresstock_amy2018_release_1%2Fmetadata%2F) to /energydata folder 

2. Run on Linux/WSL: gunicorn -k eventlet -w 1 --reload energy:app

# energy-unity.py
command line app to provide energy data for OSM buildings in JSON format (ComStock + ResStock)

1. Add .parquet files to /energydata folder. 

- [ResStock](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2021%2Fresstock_amy2018_release_1%2Fmetadata%2F) 

- [ComStock](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2021%2Fcomstock_amy2018_release_1%2Fmetadata%2F)

2. Get geojson data for desired area from [overpass API](https://overpass-turbo.eu/)

3. Run using: `py .\energy-unity.py 'weather file name (from comstock/resstock data)' 'geojson input path' 'json output path'`

e.g. `py .\energy-unity.py 'USA_CO_Denver.Internationa.725650_2018.epw' 'geojson/data-denver-new.geojson' 'json_output/buildings-energy.json'`