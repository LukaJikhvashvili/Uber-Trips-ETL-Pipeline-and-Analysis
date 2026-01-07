{{ 
  config(
    materialized = 'incremental',
    cluster_by = ['date_key'],
    unique_key = 'datetime_key'
  )
}} 

WITH date_spine AS (
  {{ dbt_utils.date_spine(
    datepart = "hour",
    start_date = " (SELECT DATE_TRUNC('hour', MIN(pickup_datetime)) FROM " ~ ref('stg_uber_trips') ~ ") ",
    end_date = " (SELECT DATE_TRUNC('hour', MAX(pickup_datetime)) + INTERVAL '1 hour' FROM " ~ ref('stg_uber_trips') ~ ") "
  ) }}
),
holidays AS (
  SELECT CAST(holiday_date AS DATE) AS holiday_date,
    holiday_name
  FROM {{ ref('seed_us_holidays') }}
)
SELECT {{ dbt_utils.generate_surrogate_key(['date_hour']) }} AS datetime_key,
  date_hour AS full_timestamp,
  DATE(date_hour) AS date_key,
  YEAR(date_hour) AS year,
  QUARTER(date_hour) AS quarter,
  MONTH(date_hour) AS month,
  DAY(date_hour) AS day_of_month,
  DAYOFWEEK(date_hour) AS day_of_week,
  -- 0=Sun, 6=Sat in Snowflake
  DAYNAME(date_hour) AS day_name,
  HOUR(date_hour) AS hour_24,
  -- Flags
  CASE
    WHEN HOUR(date_hour) BETWEEN 7 AND 9
    OR HOUR(date_hour) BETWEEN 17 AND 19 THEN TRUE
    ELSE FALSE
  END AS is_peak_hour,
  CASE
    WHEN DAYOFWEEK(date_hour) IN (0, 6) THEN TRUE
    ELSE FALSE
  END AS is_weekend,
  -- Adjusted for Snowflake (Sun=0)
  CASE
    WHEN HOUR(date_hour) BETWEEN 22 AND 23
    OR HOUR(date_hour) BETWEEN 0 AND 5 THEN TRUE
    ELSE FALSE
  END AS is_night,
  -- Holiday flag and name
  CASE
    WHEN h.holiday_date IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_holiday,
  h.holiday_name AS holiday_name
FROM date_spine
  LEFT JOIN holidays h ON DATE(date_hour) = h.holiday_date
{% if is_incremental() %}
WHERE datetime_key not in (select datetime_key from {{ this }})
{% endif %}