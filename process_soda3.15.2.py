import os
import xarray as xr
import argparse

# Default directories and date ranges
DEFAULT_INPUT_DIR = "/scratch/20cl91p02/CROCO_TOOL_FIX/Oforc_SODA"
DEFAULT_OUTPUT_DIR = "/scratch/20cl91p02/CROCO_TOOL_FIX/Oforc_SODA/output_soda"
DEFAULT_YEAR_START = 2023
DEFAULT_MONTH_START = 1
DEFAULT_YEAR_END = 2023
DEFAULT_MONTH_END = 3

def extract_variables(input_file):
    ds = xr.open_dataset(input_file)

    # Rename variables
    variable_mapping = {'u': 'uo', 'v': 'vo', 'ssh': 'zos', 'temp': 'thetao', 'salt': 'so'}
    ds = ds.rename(variable_mapping)

    # Extracting desired variables
    ds_subset = ds[['uo', 'vo', 'zos', 'thetao', 'so']]
    ds.close()
    return ds_subset

def create_monthly_files(input_dir, output_dir, start_year, end_year, start_month, end_month):
    for year in range(start_year, end_year + 1):
        input_file = f"{input_dir}/soda3.15.2_mn_ocean_reg_{year}.nc"
        
        if os.path.exists(input_file):
            for month in range(start_month, end_month + 1):
                # Construct the output filename in the desired "raw_soda_Y1993M3.nc" format
                output_file = f"{output_dir}/raw_soda_Y{year}M{month}.nc"

                ds_subset = extract_variables(input_file)

                try:
                    # Select the monthly data
                    ds_monthly = ds_subset.sel(time=f"{year}-{month:02d}")
                    ds_monthly.to_netcdf(output_file)
                    print(f"Saved {output_file}")
                except KeyError:
                    print(f"Skipping {output_file} - Date {year}-{month:02d} not found in dataset")
                except IndexError as e:
                    print(f"Skipping {output_file} - Index error: {str(e)}")
                except Exception as e:
                    print(f"Skipping {output_file} - Unexpected error: {str(e)}")
        else:
            print(f"Skipping {input_file} - File not found")

def main():
    parser = argparse.ArgumentParser(description="Process SODA3.15.2 data and create monthly files.")
    parser.add_argument("--input-dir", type=str, default=DEFAULT_INPUT_DIR, help="Input directory for yearly files")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Output directory for monthly files")
    parser.add_argument("--start-year", type=int, default=DEFAULT_YEAR_START, help="Start year")
    parser.add_argument("--end-year", type=int, default=DEFAULT_YEAR_END, help="End year")
    parser.add_argument("--start-month", type=int, default=DEFAULT_MONTH_START, help="Start month")
    parser.add_argument("--end-month", type=int, default=DEFAULT_MONTH_END, help="End month")

    args = parser.parse_args()

    create_monthly_files(args.input_dir, args.output_dir, args.start_year, args.end_year, args.start_month, args.end_month)

if __name__ == "__main__":
    main()
