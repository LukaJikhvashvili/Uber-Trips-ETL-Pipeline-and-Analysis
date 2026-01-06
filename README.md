# Uber ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline for analyzing Uber trip data. It uses dbt (data build tool) to transform raw data into a structured format for analysis and is designed to work with Snowflake.

## Project Structure

```
uber_etl_pipeline/
├── data/                  # Raw data files
├── dbt_project.yml        # dbt project configuration
├── models/                # dbt models
│   ├── marts/             # Data marts for analysis
│   └── staging/           # Staging models for data cleaning
├── seeds/                 # CSV seed files
├── tests/                 # Data quality tests
├── download_data.py       # Script to download data
├── upload_data_to_snowflake.sh # Script to upload data to Snowflake
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

2.  **Install dbt packages:**

    ```bash
    dbt deps
    ```

3.  **Set up Snowflake credentials:**
    Set the following environment variables with your Snowflake account details:
    ```bash
    export SNOWFLAKE_ACCOUNT=<your_snowflake_account>
    export SNOWFLAKE_USER=<your_snowflake_user>
    export SNOWFLAKE_PASSWORD=<your_snowflake_password>
    ```

### Running the Pipeline

1.  **Download the data:**

    ```bash
    python download_data.py
    ```

2.  **Upload the data to Snowflake:**

    ```bash
    ./upload_data_to_snowflake.sh
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

- **`stg_uber_trips`**: Cleans and prepares the raw trip data.
- **`fact_trips`**: A fact table containing detailed trip information.
- **`dim_datetime`**: A dimension table for time-based analysis.
- **`dim_location`**: A dimension table for location-based analysis.
- **`dim_trip_flags`**: A dimension table for payment-related flags.

## Data Sources

The pipeline uses the following data sources:

- **Uber Trip Data**: Parquet files containing Uber trip data, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
- **Taxi Zone Lookup**: A CSV file containing a lookup table for taxi zones, downloaded from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website.
