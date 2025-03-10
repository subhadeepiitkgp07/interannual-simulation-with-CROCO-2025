#!/usr/bin/env python

# Script to download ECMWF ERA5 reanalysis datasets from the Climate Data
#  Store (CDS) of Copernicus https://cds.climate.copernicus.eu
#  This file is part of CROCOTOOLS
#
#  CROCOTOOLS is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published
#  by the Free Software Foundation; either version 2 of the License,
#  or (at your option) any later version.
#
#  CROCOTOOLS is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#  MA  02111-1307  USA
#
#  This script use the CDS Phyton API[*] to connect and download specific ERA5 
#  variables, for a chosen area and monthly date interval, required by CROCO to 
#  perform simulations with atmospheric forcing. Furthermore, this script use 
#  ERA5 parameter names and not parameter IDs as these did not result in stable 
#  downloads. 
#
#  Tested using Python 3.8.6 and Python 3.9.1. This script need the following
#  python libraries pre-installed: "calendar", "datetime", "json" and "os".
#
#  [*] https://cds.climate.copernicus.eu/how-to-api
#
#  Copyright (c) DDONOSO February 2021
#  e-mail:ddonoso@dgeo.udec.cl  
#  Updated :: S. Maishal (2025)
#  Email   ::  subhadeepmaishal@kgpian.iitkgp.ac.in
#  You may see all available ERA5 variables at the following website
#  https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Parameterlistings

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +                                                                                                                              +
# +   Please update  ::  https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download#manage-licences   +
# +  (Licence to use Copernicus Products)                                                                                        +
# +  Terms of use show :: Accepted                                                                                               +
# +                                                                                                                              +
# +  change nano ~/.cdsapirc   add following...                                                                                  +
# +  url: https://cds.climate.copernicus.eu/api                                                                                  +
# +  key: b2cd2925-848b-443e-9417-xxxxxxxxxxxxx                                                                                  +
# +  (you will find you API Token here : https://cds.climate.copernicus.eu/profile)                                              +
# +                                                                                                                              +
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import cdsapi
import calendar
import datetime
import json
import os
from multiprocessing import Pool

# Importing utility function from ERA5_utilities
from ERA5_utilities import addmonths4date
from era5_crocotools_param import *  # Import parameters like `year_start`, `month_start`, `area`, etc.

# Print parameters for confirmation
print('Year start:', year_start)
print('Year end:', year_end)
print('Month start:', month_start)
print('Month end:', month_end)

# Adjust area if `ownArea` is not set
dl = 2
if ownArea == 0:
    with open(paramFile) as f:
        lines = f.readlines()
    for line in lines:
        if "lonmin" in line:
            lonmin = line.split('=')[1].split(';')[0].strip()
        elif "lonmax" in line:
            lonmax = line.split('=')[1].split(';')[0].strip()
        elif "latmin" in line:
            latmin = line.split('=')[1].split(';')[0].strip()
        elif "latmax" in line:
            latmax = line.split('=')[1].split(';')[0].strip()

lonmin = str(float(lonmin) - dl)
lonmax = str(float(lonmax) + dl)
latmin = str(float(latmin) - dl)
latmax = str(float(latmax) + dl)
print('Adjusted area:')
print('lonmin =', lonmin, ', lonmax =', lonmax)
print('latmin =', latmin, ', latmax =', latmax)

# Define area
area = [latmax, lonmin, latmin, lonmax]

# Ensure raw output directory exists
os.makedirs(era5_dir_raw, exist_ok=True)

# Load ERA5 variable metadata
with open('ERA5_variables.json', 'r') as jf:
    era5 = json.load(jf)


# Function to download all variables for a specific date
def download_data_by_date(year, month, area, era5, variables, era5_dir_raw, n_overlap):
    c = cdsapi.Client()

    # Number of days in the month
    days_in_month = calendar.monthrange(year, month)[1]

    # Define date range with overlap
    date_start = datetime.datetime(year, month, 1)
    date_end = datetime.datetime(year, month, days_in_month)

    n_start = datetime.date.toordinal(date_start)
    n_end = datetime.date.toordinal(date_end)

    # Overlapping date string limits
    datestr_start_overlap = datetime.date.fromordinal(n_start - n_overlap).strftime('%Y-%m-%d')
    datestr_end_overlap = datetime.date.fromordinal(n_end + n_overlap).strftime('%Y-%m-%d')
    vdate = datestr_start_overlap + '/' + datestr_end_overlap

    # Loop over variables for this date
    for vname in variables:
        vlong = era5[vname][0]
        vlevt = era5[vname][3]

        # Request options
        options = {
            'product_type': 'reanalysis',
            'type': 'an',
            'date': vdate,
            'variable': vlong,
            'levtype': vlevt,
            'area': area,
            'format': 'netcdf',
        }

        if vlong == 'sea_surface_temperature':
            options['time'] = '00'
        elif vlong == 'land_sea_mask':
            options['time'] = '00:00'
        else:
            options['time'] = time

        if vlong in ['specific_humidity', 'relative_humidity']:
            options['pressure_level'] = '1000'
            product = 'reanalysis-era5-pressure-levels'
        else:
            product = 'reanalysis-era5-single-levels'

        # Output filename
        fname = f'ERA5_ecmwf_{vname.upper()}_Y{year}M{str(month).zfill(2)}.nc'
        output = os.path.join(era5_dir_raw, fname)

        # Print info
        print(f"Downloading {vlong} for {year}-{month}...")

        # Perform the download
        try:
            c.retrieve(product, options, output)
            print(f"Downloaded {fname}")
        except Exception as e:
            print(f"Error downloading {vname} for {year}-{month}: {e}")


# Parallel processing over dates
def process_dates_in_parallel(start_year, start_month, end_year, end_month, area, era5, variables, era5_dir_raw, n_overlap):
    # Generate list of year-month combinations
    monthly_date_start = datetime.datetime(start_year, start_month, 1)
    monthly_date_end = datetime.datetime(end_year, end_month, 1)

    len_monthly_dates = (monthly_date_end.year - monthly_date_start.year) * 12 + \
                        (monthly_date_end.month - monthly_date_start.month) + 1

    dates = [(monthly_date_start.year + (monthly_date_start.month + i - 1) // 12,
              (monthly_date_start.month + i - 1) % 12 + 1)
             for i in range(len_monthly_dates)]

    # Prepare tasks
    tasks = [(year, month, area, era5, variables, era5_dir_raw, n_overlap) for year, month in dates]

    # Process dates in parallel
    with Pool() as pool:
        pool.starmap(download_data_by_date, tasks)


# Main execution
process_dates_in_parallel(
    year_start, month_start, year_end, month_end,
    area, era5, variables, era5_dir_raw, n_overlap
)

# Print completion message
print('ERA5 data request has been successfully completed!')
