{{
  config(
    materialized='table',
    unique_key='location_id'
  )
}}

select
  location_id,
  borough,
  zone,
  service_zone
from {{ ref('seed_zone_lookup') }}