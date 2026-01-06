import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Set

import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Snowflake Configuration ---
# Fetch credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWSQL_PWD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FHV_DB")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
STAGE_NAME = os.getenv("SNOWFLAKE_STAGE", f"{DATABASE}.{SCHEMA}.FHV_INTERNAL_STAGE")

# --- Data Configuration ---
BASE_FILENAME = "{year}-{month:02d}.parquet"
DEFAULT_YEAR_RANGE = os.getenv('DATA_YEAR_RANGE')

def list_files_in_stage(cursor, stage_name: str) -> Set[str]:
    """
    Lists files in a given Snowflake stage and returns a set of normalized filenames,
    stripping the '.gz' extension if present.
    """
    logging.info(f"Listing files in stage '{stage_name}'...")
    try:
        cursor.execute(f"LIST @{stage_name};")
        staged_files_raw = cursor.fetchall()
        staged_files_normalized = set()
        for row in staged_files_raw:
            staged_filename = Path(row[0]).name
            if staged_filename.endswith(".gz"):
                staged_files_normalized.add(staged_filename[:-3])
            else:
                staged_files_normalized.add(staged_filename)
        logging.info(f"Found {len(staged_files_normalized)} unique files (normalized) in stage.")
        return staged_files_normalized
    except ProgrammingError as e:
        logging.error(f"Error listing files in stage {stage_name}: {e}")
        return set()

def set_github_action_output(name: str, value: str):
    """
    Sets an output parameter for a GitHub Actions workflow by appending to the file
    specified by the GITHUB_OUTPUT environment variable.
    """
    output_file = os.getenv("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"{name}={value}\n")
    # Also print for local execution context
    logging.info(f"Setting GitHub Action output: {name}={value}")

def main():
    """
    Checks if data files for a given date range already exist in the Snowflake stage
    and sets a GitHub Action output 'download_needed' to 'true' or 'false'.
    """
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Check for new data files to upload to Snowflake.")
    parser.add_argument(
        "--years",
        type=str,
        default=DEFAULT_YEAR_RANGE,
        help=f"Year range for parquet files, e.g., '2020-2025'. Defaults to {DEFAULT_YEAR_RANGE}.",
    )
    parser.add_argument(
        "--months", type=str, default="1-12", help="Month range for parquet files, e.g., '1-12'."
    )
    args = parser.parse_args()

    # --- Generate Target File List ---
    try:
        year_start, year_end = map(int, args.years.split("-"))
        month_start, month_end = map(int, args.months.split("-"))
    except ValueError:
        logging.error("Invalid range format. Please use 'start-end', e.g., '2020-2024'.")
        sys.exit(1)
        
    target_files = set()
    for year in range(year_start, year_end + 1):
        for month in range(month_start, month_end + 1):
            target_files.add(BASE_FILENAME.format(year=year, month=month))
    
    logging.info(f"Generated {len(target_files)} target filenames to check for.")

    # --- Check for Snowflake Credentials ---
    if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
        logging.error("Snowflake credentials (SNOWFLAKE_USER, SNOWSQL_PWD, SNOWFLAKE_ACCOUNT) are not set.")
        sys.exit(1)

    # --- Compare with Staged Files ---
    try:
        with snowflake.connector.connect(
            user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
            warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA
        ) as conn:
            logging.info("Successfully connected to Snowflake.")
            cs = conn.cursor()
            staged_files = list_files_in_stage(cs, STAGE_NAME)
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake and list staged files: {e}")
        sys.exit(1)

    # Find the difference
    missing_files = target_files - staged_files
    
    if not missing_files:
        logging.info("All target files already exist in the Snowflake stage. No download needed.")
        set_github_action_output("download_needed", "false")
    else:
        logging.info(f"{len(missing_files)} new file(s) to download.")
        # Log a sample of missing files for easier debugging
        for f in sorted(list(missing_files))[:5]:
            logging.info(f"  - Missing file: {f}")
        if len(missing_files) > 5:
            logging.info(f"  - ... and {len(missing_files) - 5} more.")
        set_github_action_output("download_needed", "true")

if __name__ == "__main__":
    main()
