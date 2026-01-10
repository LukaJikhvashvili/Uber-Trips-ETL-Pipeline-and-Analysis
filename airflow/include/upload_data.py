import os
import sys
from pathlib import Path
import logging
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from typing import Set

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch credentials from environment variables, with sensible defaults for non-sensitive data
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# Use defaults that align with the original shell script and dbt project structure
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FHV_DB")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
STAGE_NAME = os.getenv("SNOWFLAKE_STAGE", f"{DATABASE}.{SCHEMA}.FHV_INTERNAL_STAGE")
FILE_FORMAT_NAME = os.getenv("SNOWFLAKE_FILE_FORMAT", f"{DATABASE}.{SCHEMA}.FHV_PARQUET_FORMAT")

# Check for required environment variables
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
    logging.error("Error: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD must be set.")
    sys.exit(1)

DATA_DIR = Path("/usr/local/airflow/data/parquet")

def execute_sql(cursor, sql_text: str, success_msg: str = ""):
    """Executes a single SQL statement."""
    try:
        cursor.execute(sql_text)
        if success_msg:
            logging.info(success_msg)
    except ProgrammingError as e:
        logging.error(f"Error executing SQL:\n{sql_text}\n{e}")
        raise

def list_files_in_stage(cursor, stage_name: str) -> Set[str]:
    """Lists files in a given Snowflake stage and returns a set of filenames."""
    logging.info(f"Listing files in stage '{stage_name}'...")
    try:
        cursor.execute(f"LIST @{stage_name};")
        staged_files_raw = cursor.fetchall()
        # The filename from LIST is the first column. Path().name extracts the final component.
        staged_files = {Path(row[0]).name for row in staged_files_raw}
        logging.info(f"Found {len(staged_files)} files in stage.")
        return staged_files
    except ProgrammingError as e:
        logging.error(f"Error listing files in stage {stage_name}: {e}")
        return set()

def main():
    """Connects to Snowflake and uploads only new Parquet files from a local directory."""
    try:
        with snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        ) as conn:
            logging.info("Successfully connected to Snowflake.")
            cs = conn.cursor()

            # --- Setup: Create Stage and File Format ---
            logging.info("--- Setting up Snowflake objects ---")
            execute_sql(cs, f"CREATE STAGE IF NOT EXISTS {STAGE_NAME};", f"Stage '{STAGE_NAME}' ensured.")
            
            create_file_format_sql = f"""
            CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT_NAME}
              TYPE = 'PARQUET'
              COMPRESSION = 'AUTO';
            """
            execute_sql(cs, create_file_format_sql, f"File format '{FILE_FORMAT_NAME}' ensured.")

            # --- Compare and Upload Files ---
            logging.info("--- Starting file upload check ---")
            
            staged_files = list_files_in_stage(cs, STAGE_NAME)
            local_files = list(DATA_DIR.rglob("*.parquet"))

            if not local_files:
                logging.warning(f"No .parquet files found in local directory '{DATA_DIR}'. Exiting.")
                return

            files_to_upload = []
            for local_file in local_files:
                # With AUTO_COMPRESS=TRUE, Snowflake adds a .gz extension to the staged file.
                # This is the critical part to correctly identify existing files.
                expected_staged_filename = f"{local_file.name}"
                if expected_staged_filename not in staged_files:
                    files_to_upload.append(local_file)
                else:
                    logging.info(f"Skipping '{local_file.name}', already exists in stage as '{expected_staged_filename}'.")
            
            if not files_to_upload:
                logging.info("All local parquet files already exist in the stage. Nothing to upload.")
                return

            logging.info(f"Found {len(files_to_upload)} new files to upload.")

            for file_path in files_to_upload:
                logging.info(f"Uploading {file_path.name}...")
                absolute_file_path = str(file_path.resolve())
                # Use POSIX path for cross-platform compatibility in Snowflake URIs
                normalized_path = absolute_file_path.replace("\\", "/")
                put_sql = f"PUT file://{normalized_path} @{STAGE_NAME} AUTO_COMPRESS=TRUE PARALLEL=16;"

                try:
                    cs.execute(put_sql)
                    # A "no results" error is expected on successful PUT, so we can't rely on simple success.
                    # The absence of a ProgrammingError other than the expected one is our success metric.
                    logging.info(f"Successfully uploaded {file_path.name}")
                except ProgrammingError as e:
                    # Per Snowflake docs, a "no results" error (253005) is expected on successful PUT.
                    # Any other error is a true failure.
                    if e.errno != 253005:
                        logging.error(f"Failed to upload {file_path.name}: {e}")
                    else:
                         logging.info(f"Successfully uploaded {file_path.name}")


            logging.info("--- All files processed. ---")

    except ProgrammingError as e:
        logging.error(f"A database error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        logging.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()