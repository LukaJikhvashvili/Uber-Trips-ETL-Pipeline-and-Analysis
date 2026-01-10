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
├── .github/                  # GitHub Actions workflows
│   └── workflows/
│       └── dbt_build.yml     # CI/CD pipeline for the project
├── .dockerignore             # Docker ignore file
├── Dockerfile                # Dockerfile for the Airflow environment
├── data/                     # Raw and processed data files
├── dbt_project.yml           # dbt project configuration
├── models/                   # dbt models
│   ├── marts/                # Data marts for analysis
│   └── staging/              # Staging models for data cleaning
├── seeds/                    # CSV seed files
├── tests/                    # Data quality tests
├── airflow/         # Airflow project specific files
│   └── dags/                 # Airflow DAGs
│       └── uber_etl_dag.py
├── download_data.py          # Script to download data from the NYC TLC website
├── upload_data.py            # Script to upload data to a Snowflake stage
├── get_data_into_raw_table.py # Script to load data from stage to raw table
├── check_for_new_data.py     # Script to check for new data to download
└── README.md                 # This file
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
    git clone https://github.com/your-username/uber_etl_pipeline.git
    cd uber_etl_pipeline
    ```

2.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    dbt deps
    ```

3.  **Set up Snowflake credentials:**
    Set the following environment variables with your Snowflake account details:
    ```bash
    export SNOWFLAKE_ACCOUNT=<your_snowflake_account>
    export SNOWFLAKE_USER=<your_snowflake_user>
    export SNOWFLAKE_PASSWORD=<your_snowflake_password>
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

The project includes a GitHub Actions workflow in `.github/workflows/dbt_build.yml` that automates the testing of the dbt project. The workflow is triggered on pushes to the `main` branch and can also be run manually. It performs the following steps:

1.  **Installs dependencies.**
2.  **Runs `dbt deps` to install dbt packages.**
3.  **Runs `dbt seed` to load seed data.**
4.  **Runs `dbt build` to build and test the dbt models.**

This ensures that the dbt project is always in a valid state.

## Data Sources

The pipeline uses the following data sources:

- **Uber Trip Data**: Parquet files containing Uber trip data, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
- **Taxi Zone Lookup**: A CSV file containing a lookup table for taxi zones, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
