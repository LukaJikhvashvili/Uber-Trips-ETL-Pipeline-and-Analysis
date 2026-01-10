{{ 
  config(
    materialized='incremental',
    unique_key='date_key'
  ) 
}}

SELECT
  dd.date_key,
  dd.year,
  dd.month,
  dd.day_of_week,
  dd.hour_24,

  pu.borough as pu_borough,
  pu.zone as pu_zone,
  do.borough as do_borough,
  do.zone as do_zone,

  COUNT(*) AS trip_count,
  AVG(trip_miles) AS avg_trip_miles,
  AVG(trip_time) AS avg_trip_time,
  AVG(base_passenger_fare) AS avg_passenger_fare,

FROM {{ ref('fact_trips') }} ft
JOIN {{ ref('dim_datetime') }} dd ON DATE_TRUNC('hour', ft.pickup_datetime) = dd.full_timestamp
JOIN {{ ref('dim_location') }} pu ON ft.pulocation_id = pu.location_id
JOIN {{ ref('dim_location') }} do ON ft.dolocation_id = do.location_id
{% if is_incremental() %}
WHERE dd.date_key not in (select date_key from {{ this }})
{% endif %}
GROUP BY
    dd.date_key, dd.year, dd.month, dd.day_of_week, dd.hour_24,
    pu_borough, pu_zone,
    do_borough, do_zone
