# Uber ETL Pipeline

This project is a dbt-based ETL pipeline for analyzing Uber trip data. It automates the process of downloading, ingesting, and transforming Uber trip data into a structured format for analysis in Snowflake.

## Features

- **Automated Data Ingestion**: Automatically downloads the latest Uber trip data in Parquet format and taxi zone lookup data from the NYC TLC website.
- **Efficient Data Loading**: Uploads data to a Snowflake internal stage, checking for existing files to avoid duplicates.
- **Incremental Data Transformation**: Uses dbt to incrementally transform and model the data, ensuring that only new or modified data is processed.
- **CI/CD Automation**: Includes a GitHub Actions workflow to automate the entire ETL process, from data downloading to dbt model building.
- **Data Quality Testing**: Includes dbt tests to ensure the integrity and quality of the transformed data.

## Project Structure

```
uber_etl_pipeline/
├── .github/               # GitHub Actions workflows
│   └── workflows/
│       └── dbt_build.yml  # CI/CD pipeline for the project
├── data/                  # Raw and processed data files
├── dbt_project.yml        # dbt project configuration
├── models/                # dbt models
│   ├── marts/             # Data marts for analysis
│   └── staging/           # Staging models for data cleaning
├── seeds/                 # CSV seed files
├── tests/                 # Data quality tests
├── download_data.py       # Script to download data from the NYC TLC website
├── upload_data.py         # Script to upload data to a Snowflake stage
├── check_for_new_data.py  # Script to check for new data to download
└── README.md              # This file
```

## Getting Started

### Prerequisites

- [dbt](https://docs.getdbt.com/docs/installation)
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

### Running the Pipeline Manually

1.  **Download the data:**

    ```bash
    python download_data.py
    ```

2.  **Upload the data to Snowflake:**

    ```bash
    python upload_data.py
    ```

3.  **Seed the dbt models:**

    ```bash
    dbt seed
    ```

4.  **Run the dbt models:**

    ```bash
    dbt run
    ```

5.  **Test the data quality:**
    ```bash
    dbt test
    ```

## Data Models

The dbt models transform the raw Uber trip data into a structured format for analysis. The main models are:

- **`stg_uber_trips`**: Cleans and prepares the raw trip data from the Snowflake stage.
- **`fact_trips`**: An incremental fact table containing detailed trip information.
- **`dim_datetime`**: A dimension table for time-based analysis.
- **`dim_location`**: A dimension table for location-based analysis.
- **`dim_trip_flags`**: A dimension table for payment-related flags.

## CI/CD Pipeline

The project includes a GitHub Actions workflow in `.github/workflows/dbt_build.yml` that automates the ETL pipeline. The workflow is triggered on pushes to the `main` branch and can also be run manually. It performs the following steps:

1.  **Checks for new data**: The `check_for_new_data.py` script checks if there is new data to download from the NYC TLC website.
2.  **Downloads new data**: If new data is available, the `download_data.py` script is run to download it.
3.  **Uploads data to Snowflake**: The new data is uploaded to a Snowflake internal stage using the `upload_data.py` script.
4.  **Runs dbt**: The dbt models are run to transform the new data. The pipeline is configured to only run modified models and their downstream dependencies.

## Data Sources

The pipeline uses the following data sources:

- **Uber Trip Data**: Parquet files containing Uber trip data, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
- **Taxi Zone Lookup**: A CSV file containing a lookup table for taxi zones, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.