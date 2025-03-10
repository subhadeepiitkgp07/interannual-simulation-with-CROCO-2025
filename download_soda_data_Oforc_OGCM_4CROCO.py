# developed by Subhadeep Maishal
# Indian Institute of Technology, Kharagpur
# subhadeepmaishal@kgpian.iitkgp.ac.in
# for more about SODA data visit (https://www2.atmos.umd.edu/%7Eocean/index_files/soda3.15.2_mn_download_b.htm)
# duration of avail 1980-2021+
# about CROCO and CROCO tools visit: https://www.croco-ocean.org/
# Install argparse (if not already installed)
#pip install argparse
import os
import argparse
import subprocess

DEFAULT_OUTPUT_DIR = "/scratch/20cl91p02/CROCO_TOOL_FIX/Oforc_SODA"
DEFAULT_YEAR_START = 2023
DEFAULT_MONTH_START = 1
DEFAULT_YEAR_END = 2023
DEFAULT_MONTH_END = 3
DEFAULT_DATA_TYPE = "monthly"  # Default data type

def download_data(url, output_dir, data_type):
    subprocess.run(['wget', '-r', '-l1', '--no-parent', '--progress=bar', '-nd', '--content-disposition', '--trust-server-names', '-A.nc', f'soda3.15.2_{data_type}_ocean_reg_*.nc', url, '-P', output_dir])

def download_monthly_data(year, output_dir, data_type):
    url = f"http://dsrs.atmos.umd.edu/DATA/soda3.15.2/REGRIDED/ocean/soda3.15.2_mn_ocean_reg_{year}.nc"
    download_data(url, output_dir, data_type)

def download_5daily_data(year, month, output_dir, data_type):
    url = f"http://dsrs.atmos.umd.edu/DATA/soda3.15.2/REGRIDED/ocean/soda3.15.2_5dy_ocean_reg_{year}_{month:02d}_*.nc"
    subprocess.run(['wget', '-r', '-l1', '--no-parent', '--progress=bar', '-nd', '--content-disposition', '--trust-server-names', '-A', f'soda3.15.2_{data_type}_ocean_reg_{year}_{month:02d}_*.nc', url, '-P', output_dir])

def main():
    parser = argparse.ArgumentParser(description="Download SODA3.15.2 data.")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Output directory for downloaded data")
    parser.add_argument("--year-start", type=int, default=DEFAULT_YEAR_START, help="Start year")
    parser.add_argument("--month-start", type=int, default=DEFAULT_MONTH_START, help="Start month")
    parser.add_argument("--year-end", type=int, default=DEFAULT_YEAR_END, help="End year")
    parser.add_argument("--month-end", type=int, default=DEFAULT_MONTH_END, help="End month")

    # Add --data-type as a choice to allow either monthly or 5daily
    data_type_choices = ["monthly", "5daily"]
    parser.add_argument("--data-type", choices=data_type_choices, default=DEFAULT_DATA_TYPE, help="Type of data to download (monthly or 5daily)")

    args = parser.parse_args()

    YEAR_START = args.year_start
    MONTH_START = args.month_start
    YEAR_END = args.year_end
    MONTH_END = args.month_end
    OUTDIR = args.output_dir
    data_type = args.data_type

    os.makedirs(OUTDIR, exist_ok=True)

    if data_type == "monthly":
        for YEAR in range(YEAR_START, YEAR_END + 1):
            download_monthly_data(YEAR, OUTDIR, data_type)
    elif data_type == "5daily":
        for YEAR in range(YEAR_START, YEAR_END + 1):
            mstart = MONTH_START if YEAR != YEAR_START else 1
            mend = MONTH_END if YEAR != YEAR_END else 12
            for MONTH in range(mstart, mend + 1):
                download_5daily_data(YEAR, MONTH, OUTDIR, data_type)

    print("Data downloaded successfully to:", OUTDIR)

if __name__ == "__main__":
    main()