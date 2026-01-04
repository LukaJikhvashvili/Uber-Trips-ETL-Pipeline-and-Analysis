{{
  config(
    materialized='table',
    unique_key='location_id'
  )
}}

select
  LocationID as location_id,
  Borough as borough,
  Zone as zone,
  service_zone as service_zone
from {{ ref('seed_zone_lookup') }}