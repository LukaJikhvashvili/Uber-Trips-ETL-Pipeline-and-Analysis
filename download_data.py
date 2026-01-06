import os
import argparse
import logging
from pathlib import Path
import requests
from tqdm import tqdm

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL_PARQUET = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"
BASE_URL_CSV = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_YEAR_RANGE = os.getenv('DATA_YEAR_RANGE', '2025-2025')  # A sensible default range

def download_file(url: str, local_path: Path):
    """
    Downloads a file from a URL to a local path with a progress bar, skipping if it already exists.

    Args:
        url (str): The URL of the file to download.
        local_path (Path): The local path to save the file.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    if local_path.exists():
        logging.info(f"File already exists, skipping: {local_path}")
        return
    
    try:
        logging.info(f"Downloading {url} to {local_path}")
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(local_path, 'wb') as f, tqdm(
                total=total_size, unit='iB', unit_scale=True, desc=local_path.name
            ) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")

def update_csv_header(csv_path: Path):
    """
    Replaces the header of the given CSV file with a dbt-friendly version if it's not already correct.

    Args:
        csv_path (Path): The path to the CSV file.
    """
    if not csv_path.exists():
        logging.warning(f"Cannot update header, file not found: {csv_path}")
        return
        
    try:
        with open(csv_path, 'r+') as f:
            first_line = f.readline()
            # Only rewrite if the header is not what we expect
            if "location_id" not in first_line.lower():
                logging.info(f"Updating header in {csv_path}")
                remaining_lines = f.read()
                f.seek(0)
                f.write("location_id,borough,zone,service_zone\n")
                f.write(remaining_lines)
            else:
                logging.info(f"Header in {csv_path} is already correct.")
    except IOError as e:
        logging.error(f"Error updating CSV header in {csv_path}: {e}")

def parse_args():
    """Parses command-line arguments for the script."""
    parser = argparse.ArgumentParser(description="Download trip data from NYC TLC website.")
    parser.add_argument(
        "--years", 
        type=str, 
        default=DEFAULT_YEAR_RANGE, 
        help=f"Year range for parquet files, e.g., '2020-2024'. Defaults to {DEFAULT_YEAR_RANGE}."
    )
    parser.add_argument(
        "--months", 
        type=str, 
        default="1-12", 
        help="Month range for parquet files, e.g., '1-12'. Defaults to all months."
    )
    return parser.parse_args()

def main():
    """Main function to coordinate downloading all data files."""
    args = parse_args()
    
    try:
        year_start, year_end = map(int, args.years.split('-'))
        month_start, month_end = map(int, args.months.split('-'))
    except ValueError:
        logging.error("Invalid range format. Please use 'start-end', e.g., '2020-2024'.")
        return

    # Download parquet files chronologically
    for year in range(year_start, year_end + 1):
        for month in range(month_start, month_end + 1):
            url = BASE_URL_PARQUET.format(year=year, month=month)
            local_path = Path("data", "parquet", str(year), f"{year}-{month:02d}.parquet")
            download_file(url, local_path)

    # Download and process the taxi zone lookup CSV
    csv_local_path = Path("seeds", "seed_zone_lookup.csv")
    download_file(BASE_URL_CSV, csv_local_path)
    update_csv_header(csv_local_path)

    logging.info("Download process completed.")

if __name__ == "__main__":
    main()