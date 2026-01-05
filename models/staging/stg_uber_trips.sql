{{
  config(
    materialized='view',
  )
}}

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
  wav_match_flag
from {{ source('raw', 'FHV_TRIPS') }}
where hvfhs_license_num = 'HV0003' -- Uber HVFHS
  and pickup_datetime is not null 
  and dropoff_datetime is not null