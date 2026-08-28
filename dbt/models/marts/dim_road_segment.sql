{{ config(materialized='table') }}

select distinct on (segment_id)
    segment_id,
    road_name,
    area_code,
    area_name,
    latitude,
    longitude,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    segment_length_km,
    reference_speed_kph,
    source_system,
    max(observed_at) over (partition by segment_id) as last_observed_at
from {{ ref('stg_traffic_observation') }}
order by segment_id, observed_at desc
