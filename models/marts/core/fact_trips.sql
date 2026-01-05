{ { config(
  materialized = 'incremental',
  cluster_by = ['pickup_datetime'],
  unique_key = 'trip_id'
) } }
select trip_id,
  request_datetime,
  on_scene_datetime,
  pickup_datetime,
  dropoff_datetime,
  pulocation_id,
  dolocation_id,
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
  { { dbt_utils.generate_surrogate_key(
    ['shared_request_flag', 'shared_match_flag', 'access_a_ride_flag', 'wav_request_flag', 'wav_match_flag']
  ) } } as trip_flags_id
from { { ref('stg_uber_trips') } } { % if is_incremental() % }
where { { dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime']) } } not in (
    select trip_id
    from { { this } }
  ) { % endif % }
order by pickup_datetime