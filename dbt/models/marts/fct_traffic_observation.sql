{{
  config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns'
  )
}}

select
    event_id,
    segment_id,
    area_code,
    area_name,
    observed_at,
    ingested_at,
    source_system,
    vehicle_type,
    segment_length_km,
    speed_kph,
    reference_speed_kph,
    traffic_volume,
    travel_time_seconds,
    source_congestion_level,
    source_payload_sha256,
    speed_index,
    congestion_level,
    is_late,
    loaded_at
from {{ ref('stg_traffic_observation') }}

{% if is_incremental() %}
where loaded_at >= (select coalesce(max(loaded_at), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}
