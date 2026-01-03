import os
import requests
from tqdm import tqdm

BASE_URL_PARQUET = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"
BASE_URL_CSV = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download_file(url, local_path):
    """Downloads a file from a URL to a local path, skipping if it already exists."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if os.path.exists(local_path):
        print(f"File already exists, skipping: {local_path}")
        return
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(local_path, 'wb') as f, tqdm(
                total=total_size, unit='iB', unit_scale=True, desc=os.path.basename(local_path)
            ) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")


def main():
    """Downloads parquet and CSV data."""
    # Download parquet files from 2025 to 2020
    for year in range(2025, 2025 - 1, -1):
        for month in range(1, 13):
            file_name = f"{year}-{month:02d}.parquet"
            url = BASE_URL_PARQUET.format(year=year, month=month)
            local_path = os.path.join("data", "parquet", str(year), file_name)
            print(f"Downloading {url} to {local_path}")
            download_file(url, local_path)

    # Download CSV file
    csv_url = BASE_URL_CSV
    csv_local_path = os.path.join("data", "csv", "taxi_zone_lookup.csv")
    print(f"Downloading {csv_url} to {csv_local_path}")
    download_file(csv_url, csv_local_path)

    print("Download completed.")

if __name__ == "__main__":
    main()
