#!/usr/bin/env python3

# Library of useful functions
# Authors: Denisse Fierro Arcos and Tormey Reimer
# Date last update: 2024-09-06

# Loading libraries
import xarray as xr
import zarr
import pandas as pd
import datetime as dt
import os
import geopandas as gpd
import rioxarray
import numpy as np

# Converting downloaded WOA data to useable format ----
# Consolidates netCDF files downloaded from WOA converts them into 
# cloud-friendly zarr files.
# Defining function to save netCDF files to zarr files
def netcdf_to_zarr(list_files, var_name, file_out):
    '''
    Inputs:
    list_files (character list) Full paths to WOA files that need to be 
    merged and save as a single zarr collection. All files must contain 
    the same variable of interest.
    var_name (character) Name of variable to be saved as zarr file.
    file_out (character) Full file path where zarr file will be saved.

    Outputs:
    None. This function saves a zarr file to disk.
    '''
    da = xr.open_mfdataset(list_files, decode_times = False)[var_name]
    
    # Fix time coordinate variable - Keep name of month only
    if len(da.time) == 12:
        units, reference_date = da.time.attrs['units'].split('since')
        da['time'] = pd.date_range(start = reference_date, 
                                   periods = da.sizes['time'], 
                                   freq = 'MS').strftime('%B')
        da = da.rename({'time': 'month'})
        #Rechunk data 
        da_rechunk = da.chunk({'month': 12, 'depth': 57, 'lat': 120, 
                               'lon': 240})
    elif len(da.time) == 1:
        #Removing time dimension because it is not needed
        da = da.isel(time = 0)
        da = da.reset_coords('time', drop = True)
        #Rechunk data 
        da_rechunk = da.chunk({'depth': 57, 'lat': 120, 'lon': 240})

    #Save data, but ensure directory exists
    out_dir = os.path.dirname(file_out)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    da_rechunk.to_zarr(file_out, consolidated = True, mode = 'w')

#Defining functions
def mask_boolean_ard_data(file_path, boolean_mask_ras):
    '''
    Open netCDF files in analysis ready data (ARD) format. That is apply chunks
    that make data analysis easier.
    
    Inputs:
    file_path (character): Full file path where netCDF file is stored.
    boolean_mask_ras (boolean data array): Data array to be used as initial mask
    to decrease the size of the original dataset. This mask makes no distinction
    between regional models, it simply identifies grid cells within regional 
    model boundaries with the value of 1.
    
    Outputs:
    da (data array): ARD data array containing data only for grid cells within
    regional model boundaries.
    '''

    #Getting chunks from gridded mask to apply it to model data array
    [lat_chunk] = np.unique(boolean_mask_ras.chunksizes['lat'])
    [lon_chunk] = np.unique(boolean_mask_ras.chunksizes['lon'])
    
    #Open data array
    da = xr.open_zarr(file_path)
    [var] = list(da.keys())
    da = da[var]
    #Ensure chunks are the same as mask
    if 'month' in da.dims:
        da = da.chunk({'month': 12, 'depth': 57, 'lat': lat_chunk, 'lon': lon_chunk})
        #Make sure months are in correct format
        da['month'] = pd.date_range(start = '2010-01-01', periods = 12, 
                                    freq = 'MS').strftime('%B')
    else:
        da = da.chunk({'depth': 57, 'lat': lat_chunk, 'lon': lon_chunk})
        
    #Apply mask for all regions to decrease dataset size
    da = da.where(boolean_mask_ras == 1)
    
    #Add spatial information to dataset
    da.rio.set_spatial_dims(x_dim = 'lon', y_dim = 'lat', inplace = True)
    da.rio.write_crs('epsg:4326', inplace = True)

    return da

def mask_ard_data(ard_da, shp_mask, file_out):
    '''
    Open netCDF files in analysis ready data (ARD) format. That is apply chunks
    that make data analysis easier.
    
    Inputs:
    ard_da (data array): Analysis ready data (ARD) data array produced by the 
    function "open_ard_data"
    shp_mask (shapefile): Shapefile containing the boundaries of regional models
    file_out (character): Full file path where masked data should be stored.
    
    Outputs:
    No data is returned, but masked file will be stored in specified file path.
    '''

    #Clip data using regional shapefile
    da_mask = ard_da.rio.clip(shp_mask.geometry, shp_mask.crs, drop = True, 
                              all_touched = True)
    #Remove spatial information
    da_mask = da_mask.drop_vars('crs')
    da_mask.encoding = {}

    #Check file extension included in path to save data
    if file_out.endswith('zarr'):
        for i, c in enumerate(da_mask.chunks):
            if len(c) > 1 and len(set(c)) > 1:
                print(f'Rechunking {file_out}.')
                print(f'Dimension "{da_mask.dims[i]}" has unequal chunks.')
                da_mask = da_mask.chunk({da_mask.dims[i]: '200MB'})
        da_mask.to_zarr(file_out, consolidated = True, mode = 'w')
    if file_out.endswith('parquet'):
        #Keep data array attributes to be recorded in final data frame
        da_attrs = ard_da.attrs
        da_attrs = pd.DataFrame([da_attrs])
        ind_wider = ['lat', 'lon', 'depth', 'vals']
        #Turn extracted data into data frame and remove rows with NA values
        df = da_mask.to_series().to_frame().reset_index().dropna()
        #Changing column name to standardise across variables
        df = df.rename(columns = {ard_da.name: 'vals'}).reset_index(drop = True)
        #Reorganise data
        df = df[ind_wider]
        #Include original dataset attributes
        df = pd.concat([df, da_attrs], axis = 1)
        #Saving data frame
        df.to_parquet(file_out)
