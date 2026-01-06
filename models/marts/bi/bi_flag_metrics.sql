{{ 
  config(
    materialized = 'incremental',
    unique_key='date_key'
  ) 
}}

SELECT 
  dd.date_key,
  COUNT(*) AS total_trips,
  SUM(
    CASE
      WHEN shared_request_flag THEN 1
      ELSE 0
    END
  ) AS total_shared_rides_requested,
  SUM(
    CASE
      WHEN shared_match_flag THEN 1
      ELSE 0
    END
  ) AS total_shared_rides_matched,
  SUM(
    CASE
      WHEN wav_request_flag THEN 1
      ELSE 0
    END
  ) AS total_wav_requested,
  SUM(
    CASE
      WHEN wav_match_flag THEN 1
      ELSE 0
    END
  ) AS total_wav_matched,
  SUM(
    CASE
      WHEN access_a_ride_flag THEN 1
      ELSE 0
    END
  ) AS total_access_a_ride_requested
FROM {{ ref('fact_trips') }}
 ft
  JOIN {{ ref('dim_datetime') }}
   dd ON DATE_TRUNC('hour', ft.pickup_datetime) = dd.full_timestamp
  JOIN {{ ref('dim_trip_flags') }}
   pf ON ft.trip_flags_id = pf.trip_flags_id
WHERE ft.trip_miles > 0
  AND ft.trip_time > 0
GROUP BY dd.date_key