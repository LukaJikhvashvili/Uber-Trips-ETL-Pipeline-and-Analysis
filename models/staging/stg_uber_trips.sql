{{
  config(
    materialized='incremental',
    cluster_by = ['pickup_datetime'],
    unique_key = 'trip_id'
  )
}}

{% if is_incremental() %}
with max_ingestion as (
    select 
      max(ingestion_ts) as max_ingestion_ts 
    from {{ this }}
)
{% endif %}
select
  -- identifiers
  {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime']) }} as trip_id,
  -- timestamps
  request_datetime,
  on_scene_datetime,
  pickup_datetime,
  dropoff_datetime,
  -- locations
  PULocationID as pulocation_id,
  DOLocationID as dolocation_id,
  -- measures
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  bcf,
  sales_tax,
  congestion_surcharge,
  airport_fee,
  tips,
  driver_pay,
  cbd_congestion_fee,
  -- flags
  shared_request_flag,
  shared_match_flag,
  access_a_ride_flag,
  wav_request_flag,
  wav_match_flag,
  ingestion_ts
from {{ source('raw', 'FHV_TRIPS') }}
{% if is_incremental() %}
join max_ingestion on ingestion_ts > max_ingestion_ts
{% endif %}
where hvfhs_license_num = 'HV0003' -- Uber HVFHS
  and pickup_datetime is not null 
  and dropoff_datetime is not null
