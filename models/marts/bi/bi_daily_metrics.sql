{{ 
  config(
    materialized = 'incremental',
    unique_key='date_key'
  ) 
}}

SELECT
  dd.date_key,
  COUNT(*) AS total_trips,
  SUM(trip_miles) AS total_trip_miles,
  SUM(trip_time) / (3600 * 24) AS total_trip_time_days,
  SUM(
    base_passenger_fare + tips + tolls + airport_fee + congestion_surcharge + cbd_congestion_fee + sales_tax + bcf
  ) AS passenger_total_spend,
  SUM(
    base_passenger_fare + congestion_surcharge + cbd_congestion_fee
  ) AS platform_gross_revenue,
  SUM(driver_pay + tips) AS driver_total_earnings,
  SUM(
    sales_tax + bcf + congestion_surcharge + cbd_congestion_fee + airport_fee
  ) AS fees_and_taxes,
  SUM(
    (
      base_passenger_fare + congestion_surcharge + cbd_congestion_fee
    ) - driver_pay
  ) AS platform_net_earnings,
  SUM(
    CASE
      WHEN pf.shared_match_flag THEN 1
      ELSE 0
    END
  ) / COUNT(*) * 100 AS percent_shared_rides
FROM {{ ref('fact_trips') }} ft
  JOIN {{ ref('dim_datetime') }} dd ON DATE_TRUNC('hour', ft.pickup_datetime) = dd.full_timestamp
  JOIN {{ ref('dim_trip_flags') }} pf ON ft.trip_flags_id = pf.trip_flags_id
GROUP BY dd.date_key