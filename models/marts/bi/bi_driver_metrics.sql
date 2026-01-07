{{ 
  config(
    materialized = 'incremental',
    unique_key='date_key'
  ) 
}}

SELECT
  dd.date_key,
  AVG(trip_miles) AS avg_trip_miles,
  AVG(trip_time) AS avg_trip_time_seconds,
  AVG(driver_pay) AS avg_driver_pay,
  AVG(tips) AS avg_tips,
  AVG(driver_pay + tips) AS avg_driver_total,
  AVG(driver_pay) / AVG(trip_miles) AS avg_pay_per_mile,
  AVG(driver_pay) / (AVG(trip_time) / 3600.0) AS avg_pay_per_hour
FROM {{ ref('fact_trips') }} ft
  JOIN {{ ref('dim_datetime') }} dd ON DATE_TRUNC('hour', ft.pickup_datetime) = dd.full_timestamp
  JOIN {{ ref('dim_trip_flags') }} pf ON ft.trip_flags_id = pf.trip_flags_id
WHERE ft.trip_miles > 0
  AND ft.trip_time > 0
{% if is_incremental() %}
  AND dd.date_key not in (select date_key from {{ this }})
{% endif %}
GROUP BY dd.date_key