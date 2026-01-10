import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DATA_YEAR_RANGE = os.getenv("DATA_YEAR_RANGE", "2025-2026")

with DAG(
    dag_id="uber_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={ "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["tlc", "hvfhs", "uber", "dbt", "snowflake"],
    doc_md="""
    # NYC TLC HVFHV (Uber-focused) Monthly ETL Pipeline

    This DAG automates the full monthly refresh:
    - Checks for new HVFHV Parquet files
    - Downloads them
    - Uploads to Snowflake stage
    - Loads into raw table
    - Runs dbt models and tests
    """,
) as dag:

    current_year = datetime.now().year
    start_year = int(DATA_YEAR_RANGE.split("-")[0])
    years = f"{start_year}-{current_year}"


    CWD = '/usr/local/airflow/'

    check_for_new_data = BashOperator(
        task_id="check_for_new_data",
        cwd=CWD,
        bash_command=f"python include/check_for_new_data.py --years={years}",
        doc_md="""
        ### Check for New Data Availability

        Checks which monthly HVFHV Parquet files are missing from the Snowflake stage
        and outputs the list of missing dates (e.g., '2024-01,2024-02') to XComs.
        """,
        do_xcom_push=True,
    )

    download_data = BashOperator(
        task_id="download_data",
        cwd=CWD,
        bash_command="""
        missing_dates_output="{{ task_instance.xcom_pull(task_ids='check_for_new_data', key='return_value') }}"
        if [[ "$missing_dates_output" == "missing_dates="* ]]; then
            missing_dates=$(echo "$missing_dates_output" | cut -d'=' -f2)
            if [[ -n "$missing_dates" ]]; then
                python include/download_data.py --dates "$missing_dates"
            else
                echo "No new dates to download."
            fi
        else
            echo "No missing dates output from previous task."
        fi
        """,
        doc_md="""
        ### Download HVFHV Parquet Files

        Downloads missing High-Volume FHV trip data based on the dates
        provided by the `check_for_new_data` task.
        """,
    )

    upload_data_to_stage = BashOperator(
        task_id="upload_data_to_stage",
        cwd=CWD,
        bash_command=f"python include/upload_data.py",
        doc_md="""
        ### Upload Files to Snowflake Internal Stage

        Uses SnowSQL to PUT local Parquet files into the Snowflake internal stage.
        """,
    )

    load_raw_table = BashOperator(
        task_id="load_raw_table",
        cwd=CWD,
        bash_command=f"python include/get_data_into_raw_table.py",
        doc_md="""
        ### Copy Data from Stage into Raw Table

        Executes COPY INTO to load new Parquet files into the raw HVFHV_TRIPS table.
        Uses MATCH_BY_COLUMN_NAME and logical type support for correct timestamps.
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        cwd=CWD,
        bash_command=f"cd dbt && dbt run",
        doc_md="""
        ### Transform Data with dbt

        Runs all dbt models: staging → marts (fact/dim tables).
        Builds clustered, optimized tables in the MART schema.
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=CWD,
        bash_command=f"cd dbt && dbt test",
        doc_md="""
        ### Validate Data Quality

        Runs all dbt tests (not_null, unique, relationships, freshness, etc.).
        """,
    )


    check_for_new_data >> download_data >> upload_data_to_stage >> load_raw_table >> dbt_run >> dbt_test
