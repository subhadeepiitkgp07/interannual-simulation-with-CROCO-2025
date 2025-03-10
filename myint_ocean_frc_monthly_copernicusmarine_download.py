#__author__ = 'S. Maishal'
#__email__  = 'subhadeepmaishal@kgpian.iitkgp.ac.in'
#__date__   = '2024-08-20'
#===========================================================================
#Further Information:  
#  http://www.croco-ocean.org
  
#This file is part of CROCOTOOLS

import xarray as xr
import os
import re
import subprocess
from datetime import datetime, timedelta
import calendar

#user nedd to change ========================================================
# Configuration user modification ON

data_dir = '/scratch/20cl91p02/X-TOY'
dataset_id = "cmems_mod_glo_phy_myint_0.083deg_P1M-m"
lon_min, lon_max = 86, 92
lat_min, lat_max = 20, 23
depth_min, depth_max = 0.493, 5727.918
variables = ["zos", "uo", "vo", "thetao", "so"]

# Date range for the download
YEAR_START = 2023
MONTH_START = 10
DAY_START = 1
YEAR_END = 2023
MONTH_END = 12
DAY_END = 30

# Configuration  user modification OFF 

# Function to download data for a specific variable
def download_variable(variable, start_str, end_str):
    command = [
        "copernicusmarine", "subset",
        "--dataset-id", dataset_id,
        "--variable", variable,
        "--minimum-longitude", str(lon_min),
        "--maximum-longitude", str(lon_max),
        "--minimum-latitude", str(lat_min),
        "--maximum-latitude", str(lat_max),
        "--start-datetime", start_str,
        "--end-datetime", end_str,
        "--minimum-depth", str(depth_min),
        "--maximum-depth", str(depth_max)
    ]
    
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate(input='y\n')

    if process.returncode != 0:
        print(f"Failed to download data for {variable} from {start_str} to {end_str}: {stderr}")
    else:
        print(f"Successfully downloaded data for {variable} from {start_str} to {end_str}")

# Function to process and concatenate NetCDF files
def concatenate_files():
    pattern = re.compile(r'cmems_mod_glo_phy_my_0.083deg_P1M-m_(\w+)_86\.00E-92\.00E_20\.00N-23\.00N_\d+\.\d+-\d+\.\d+m_(\d{4})-(\d{2})-(\d{2})\.nc')

    files = os.listdir(data_dir)
    matching_files = [f for f in files if pattern.match(f)]
    files_by_date = {}

    for file in matching_files:
        match = pattern.match(file)
        variable, year, month, day = match.groups()
        date_key = f"{year}-{month}-{day}"
        
        if date_key not in files_by_date:
            files_by_date[date_key] = []
        
        files_by_date[date_key].append(os.path.join(data_dir, file))

    for date_key, file_list in files_by_date.items():
        if not file_list:
            print(f"No files found for date {date_key}")
            continue

        datasets = []
        try:
            datasets = [xr.open_dataset(f) for f in file_list]
            combined = xr.concat(datasets, dim='time')
            
            year, month, _ = date_key.split('-')
            output_filename = f'raw_motu_mercator_Y{year}M{month}.nc'
            combined.to_netcdf(os.path.join(data_dir, output_filename))
            print(f"Successfully created {output_filename}")

        except Exception as e:
            print(f"Failed to concatenate files for date {date_key}: {e}")

        finally:
            # Ensure datasets are closed even if an error occurs
            for ds in datasets:
                ds.close()

# Function to remove temporary files
def remove_temp_files():
    pattern = re.compile(r'cmems_mod_glo_phy_my.*\.nc')
    files = os.listdir(data_dir)
    
    for file in files:
        if pattern.match(file):
            file_path = os.path.join(data_dir, file)
            os.remove(file_path)
            print(f"Removed {file_path}")

# Main script execution
if __name__ == "__main__":
    start_date = datetime(YEAR_START, MONTH_START, DAY_START)
    end_date = datetime(YEAR_END, MONTH_END, DAY_END)

    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day
        _, last_day = calendar.monthrange(year, month)

        start_str = current_date.strftime('%Y-%m-%dT00:00:00')
        end_str = datetime(year, month, last_day).strftime('%Y-%m-%dT00:00:00')

        for variable in variables:
            try:
                download_variable(variable, start_str, end_str)
            except Exception as e:
                print(f"Failed to download data for {variable} from {start_str} to {end_str}: {e}")

        concatenate_files()
        remove_temp_files()  # Remove temporary files after concatenation

        current_date = datetime(year, month, last_day) + timedelta(days=1)

    print("=========== Download and concatenation completed! ===========")
