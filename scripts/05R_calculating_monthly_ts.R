# Calculating climatologies and times series from WOA data -----
#
# Author: Denisse Fierro Arcos
# Date: 2024-09-10
#  

## Loading libraries ----
library(arrow)
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(Hmisc)

# Defining base variables -------------------------------------------------
#Location of parquet files
woa_dir <- "/g/data/vf71/WOA_data/regional/monthly"

#Folder where mean climatologies with all data will be saved
base_out_ts <- file.path(woa_dir, "comp_clim")
if(!dir.exists(base_out_ts)){
  dir.create(base_out_ts)
}

#Getting list of parquet files available
woa_list <- list.files(woa_dir, pattern = "parquet$", full.names = T)

#Loading area file
area_df <- file.path("/g/data/vf71/shared_resources/grid_cell_area_ESMs/isimip3a",
                     "gfdl-mom6-cobalt2_areacello_15arcmin_global_fixed.csv") |> 
  read_csv() |> 
  rename(lon = x, lat = y)

  
# Calculating mean over time ----------------------------------------------
mean_ts <- function(file_path, weights_df, folder_out = NULL){
  #Inputs:
  # file_path (character): Full file path to parquet file to calculate 
  # climatology
  # weights_df (data frame): If provided, mean climatology will be weighted 
  # using these values
  # folder_out (character): Optional. Full path to folder where climatology 
  # will be saved
  
  #Load file
  df <- read_parquet(file_path)
  
  #Getting metadata
  meta <- df |> 
    select(!lat:vals) |> 
    drop_na()
  
  #Check if weights exist
  ts_df <- df |>
    left_join(weights_df, join_by(lon, lat)) |> 
    group_by(month) |> 
    summarise(weighted_mean = weighted.mean(vals, cellareao, na.rm = T),
              weighted_sd = sqrt(wtd.var(vals, cellareao, na.rm = T)))
  
  #Adding metadata
  ts_df <- ts_df |> 
    bind_cols(meta)
  
  if(!is.null(folder_out)){
    #Create file name to save climatology - if path provided
    base_name <- basename(file_path) |> 
      str_replace("month_clim_mean", "weighted_mean_ts")
    #Save file
    ts_df |>
      write_parquet(file.path(folder_out, base_name))
  }
  
  return(ts_df)
}


# Apply function to calculate timeseries ----------------------------------
woa_list |> 
  map(\(x) mean_ts(x, area_df, base_out_ts))
