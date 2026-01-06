import os
import sys
import logging
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FHV_DB")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
TABLE_NAME = "FHV_TRIPS"
STAGE_NAME = os.getenv("SNOWFLAKE_STAGE", f"{DATABASE}.{SCHEMA}.FHV_INTERNAL_STAGE")
FILE_FORMAT_NAME = os.getenv("SNOWFLAKE_FILE_FORMAT", f"{DATABASE}.{SCHEMA}.FHV_PARQUET_FORMAT")

# Check for required environment variables
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
    logging.error("Error: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD must be set.")
    sys.exit(1)

def execute_sql(cursor, sql_text: str, success_msg: str = ""):
    """Executes a single SQL statement."""
    try:
        cursor.execute(sql_text)
        if success_msg:
            logging.info(success_msg)
    except ProgrammingError as e:
        logging.error(f"Error executing SQL:\n{sql_text}\n{e}")
        raise

def main():
    """Connects to Snowflake, creates the raw table, and loads data from the stage."""
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

            # --- Setup: Create Table ---
            logging.info("-- Setting up Snowflake objects --")
            create_table_sql = f"""
            CREATE  TABLE IF NOT EXISTS {TABLE_NAME} (
                hvfhs_license_num VARCHAR(6),
                request_datetime TIMESTAMP_NTZ,
                on_scene_datetime TIMESTAMP_NTZ,
                pickup_datetime TIMESTAMP_NTZ,
                dropoff_datetime TIMESTAMP_NTZ,
                PULocationID NUMBER(3),
                DOLocationID NUMBER(3),
                trip_miles FLOAT,
                trip_time NUMBER,
                base_passenger_fare FLOAT,
                tolls FLOAT,
                bcf FLOAT,
                sales_tax FLOAT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT,
                tips FLOAT,
                driver_pay FLOAT,
                cbd_congestion_fee FLOAT DEFAULT 0.0,
                shared_request_flag BOOLEAN,
                shared_match_flag BOOLEAN,
                access_a_ride_flag BOOLEAN,
                wav_request_flag BOOLEAN,
                wav_match_flag BOOLEAN,
                -- metadata
                ingestion_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
            )
            CLUSTER BY (TO_DATE(pickup_datetime));
            """
            execute_sql(cs, create_table_sql, f"Table '{TABLE_NAME}' ensured.")

            # --- Load Data from Stage ---
            logging.info("-- Starting data load from stage --")
            copy_sql = f"""
            COPY INTO {TABLE_NAME} FROM (
                SELECT
                    $1:hvfhs_license_num::VARCHAR(6),                                                                                   
                    $1:request_datetime::TIMESTAMP_NTZ,                                                                                    
                    $1:on_scene_datetime::TIMESTAMP_NTZ,                                                                                   
                    $1:pickup_datetime::TIMESTAMP_NTZ,                                                                                     
                    $1:dropoff_datetime::TIMESTAMP_NTZ,                                                                                    
                    $1:PULocationID::INT,                                                                                 
                    $1:DOLocationID::INT,                                                                                              
                    $1:trip_miles::FLOAT,                                                                                              
                    $1:trip_time::INT,                                                                                                 
                    $1:base_passenger_fare::FLOAT,
                    $1:tolls::FLOAT,
                    $1:bcf::FLOAT,
                    $1:sales_tax::FLOAT,
                    $1:congestion_surcharge::FLOAT,
                    $1:airport_fee::FLOAT,
                    $1:tips::FLOAT,
                    $1:driver_pay::FLOAT,
                    $1:cbd_congestion_surcharge::FLOAT,
                    $1:shared_request_flag::STRING,
                    $1:shared_match_flag::STRING,
                    $1:access_a_ride_flag::STRING,
                    $1:wav_request_flag::STRING,
                    $1:wav_match_flag::STRING,
                    CURRENT_TIMESTAMP() AS ingestion_ts
                FROM @{STAGE_NAME}
            )
            PATTERN = '.*\.parquet'
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_NAME})
            ON_ERROR = 'SKIP_FILE';
            """
            execute_sql(cs, copy_sql, "Successfully loaded data from stage into raw table.")

            logging.info("-- Data loading process completed. --")

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