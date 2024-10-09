# Accessing and processing World Ocean Atlas (WOA) data programatically.
Collection of notebooks showing how to download and process World Ocean Atlas data with R and Python. This workflow includes scripts showing how to transform `netCDF` files into cloud-optimised formats such as `zarr` for gridded data and `parquet` for tabular data.

## Table of contents
All files listed below can be foun under the `scripts` folder. The first two digits in the script name indicate the order they should be run. The letter following the two digits in the name indicate whether the script was developed in Python (`P`) or R (`R`):  
- [Access and clip WOA data using Python](scripts/01P_access_clip_WOA.ipynb): In this Jupyter notebook, we use Python to access WOA18 data from NOAA's THREDDS server, clip it using a shapefile, and save the data clipped for our area of interest.  
- [Download WOA data using R](scripts/01R_download_WOA_data.R): In this R script, we download WOA23 data from NOAA's THREDDS server.
- [Transform `netCDF` files into `zarr`](scripts/02P_WOA_netcdf_to_zarr.ipynb): In this Jupyter notebook, we use Python to transform the WOA23 `netCDF` files into `zarr`, a file format for gridded data that is optimised for cloud computing.
- [Extract WOA23 data for FishMIP regions](scripts/03P_WOA_zarr_data_extraction.ipynb): In this Jupyter notebook, we use Python to extract WOA23 data from cloud optimised files produced by the [02P_WOA_netcdf_to_zarr](scripts/02P_WOA_netcdf_to_zarr.ipynb) script.
- [Getting metadata from WOA23 files](scripts/04P_compiling_WOA_metadata.ipynb): In this Jupyter notebook, we use Python to extract metadata from the original WOA23 `netcdf` files, which can be used at a later date to interpret data.  
- [Calculating monthly timeseries using R](scripts/05R_calculating_monthly_ts.R): In this R script, we calculate the monthly timeseries from WOA data.
- [Regridding WOA data](scripts/P_regridding_woa_data.ipynb): In this Jupyter notebook, we use Python to show how to calculate a climatology and how interpolate depth bins and regrid data.  
- [Useful functions](scripts/useful_functions.py): This Python script contains custom-made functions that are used in the Jupyter notebooks.

## Contributors
- [Denisse Fierro Arcos](https://github.com/lidefi87)  
- [Tormey Reimer](https://github.com/stormeyseas)  

You can contribute a Python or R script to this repository by submitting a Pull Request.  

## Have questions or suggestions?
If you have an idea for a new script, or if you spotted an error, please create an [Issue](https://github.com/Fish-MIP/processing_WOA_data/issues) or [email us](mailto:fishmip.coordinators@gmail.com).  
