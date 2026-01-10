# Uber ETL Pipeline

This project is a dbt-based ETL pipeline for analyzing Uber trip data. It automates the process of downloading, ingesting, and transforming Uber trip data into a structured format for analysis in Snowflake. The pipeline is orchestrated using Apache Airflow.

## Features

- **Automated Data Ingestion**: Automatically downloads the latest Uber trip data in Parquet format and taxi zone lookup data from the NYC TLC website.
- **Efficient Data Loading**: Uploads data to a Snowflake internal stage, checking for existing files to avoid duplicates.
- **Incremental Data Transformation**: Uses dbt to incrementally transform and model the data, ensuring that only new or modified data is processed.
- **Orchestration with Airflow**: Includes an Airflow DAG to orchestrate the entire ETL process, from data downloading to dbt model building and testing.
- **CI/CD Automation**: Includes a GitHub Actions workflow to automate the testing and validation of the dbt project.
- **Data Quality Testing**: Includes dbt tests to ensure the integrity and quality of the transformed data.

## Project Structure

```
uber_etl_pipeline/
├── .dockerignore
├── .gitignore
├── Dockerfile
├── README.md
├── requirements.txt
├── .astro/
│   └── config.yaml
├── dags/
│   └── uber_etl_dag.py
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── marts/
│   │   │   ├── bi/
│   │   │   └── core/
│   │   └── staging/
│   ├── seeds/
│   └── ...
├── include/
│   ├── check_for_new_data.py
│   ├── download_data.py
│   ├── get_data_into_raw_table.py
│   └── upload_data.py
└── ...
```

## Getting Started

### Prerequisites

- [dbt](https://docs.getdbt.com/docs/installation)
- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- [Snowflake Account](https://www.snowflake.com/)
- Python 3.x

### Setup

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/LukaJikhvashvili/Uber-Trips-ETL-Pipeline-and-Analysis.git
    cd Uber-Trips-ETL-Pipeline-and-Analysis
    ```

2.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    dbt deps
    ```

3.  **Set up Snowflake credentials:**
    Create the .env file and set the following environment variables with your Snowflake account details:
    ```bash
    SNOWFLAKE_ACCOUNT=
    SNOWFLAKE_USER=
    SNOWFLAKE_PASSWORD=
    SNOWFLAKE_ROLE=
    SNOWFLAKE_WAREHOUSE=
    SNOWFLAKE_DATABASE=
    SNOWFLAKE_SCHEMA=<schema_where_the_internal_file_stage_is_located>
    SNOWFLAKE_STAGE=<name_of_the_internal_stage_for_data_upload>
    SNOWFLAKE_FILE_FORMAT=<parquet_file_format>
    DATA_YEAR_RANGE=<year_range_to_download_data> # e.g. (2020-2026)
    ```

### Running the Pipeline with Airflow

The ETL pipeline is orchestrated using the `uber_etl_dag` Airflow DAG. To run the pipeline, you need to start the Airflow environment using the Astro CLI from the root of the project.

1.  **Start the Airflow environment:**

    ```bash
    astro dev start
    ```

2.  **Access the Airflow UI:**

    Open your browser and navigate to `http://localhost:8080`.

3.  **Trigger the DAG:**

    In the Airflow UI, you will see the `uber_etl_dag`. You can trigger it manually to run the pipeline.

## Data Models

The dbt models transform the raw Uber trip data into a structured format for analysis. The main models are:

- **`stg_uber_trips`**: Cleans and prepares the raw trip data from the Snowflake stage.
- **`fact_trips`**: An incremental fact table containing detailed trip information.
- **`dim_datetime`**: A dimension table for time-based analysis.
- **`dim_location`**: A dimension table for location-based analysis.
- **`dim_trip_flags`**: A dimension table for payment-related flags.

## CI/CD Pipeline

The project includes a GitHub Actions workflow in `.github/workflows/ci.yml` that automates the testing of the dbt project on push to the `main` branch.

This workflow uses a "slim CI" approach. It intelligently compares the changes in the pull request against the `main` branch to identify only the dbt models that have been modified. It then runs `dbt build` on just those modified models and their downstream dependencies. This significantly speeds up CI runs by avoiding the need to build and test the entire dbt project on every change.

## Data Sources

The pipeline uses the following data sources:

- **Uber Trip Data**: Parquet files containing Uber trip data, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
- **Taxi Zone Lookup**: A CSV file containing a lookup table for taxi zones, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
