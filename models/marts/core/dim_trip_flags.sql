{{
  config(materialized='table')
}}

-- generate all possible combinations of payment flags

with combinations AS (
  SELECT * FROM (
    SELECT CAST(column1 AS BOOLEAN) AS shared_request_flag FROM (VALUES (TRUE), (FALSE))
  ) a
  CROSS JOIN (
    SELECT CAST(column1 AS BOOLEAN) AS shared_match_flag FROM (VALUES (TRUE), (FALSE))
  ) b
  CROSS JOIN (
    SELECT CAST(column1 AS BOOLEAN) AS access_a_ride_flag FROM (VALUES (TRUE), (FALSE))
  ) c
  CROSS JOIN (
    SELECT CAST(column1 AS BOOLEAN) AS wav_request_flag FROM (VALUES (TRUE), (FALSE))
  ) d
  CROSS JOIN (
    SELECT CAST(column1 AS BOOLEAN) AS wav_match_flag FROM (VALUES (TRUE), (FALSE))
  ) e
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['shared_request_flag', 'shared_match_flag', 'access_a_ride_flag', 'wav_request_flag', 'wav_match_flag']) }} AS trip_flags_id,
  shared_request_flag,
  shared_match_flag,
  access_a_ride_flag,
  wav_request_flag,
  wav_match_flag
FROM combinations