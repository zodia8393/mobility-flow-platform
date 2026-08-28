with source as (
    select * from {{ source('raw', 'traffic_observation') }}
),

renamed as (
    select
        event_id,
        trace_id,
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
        schema_version,
        loaded_at
    from source
)

select * from renamed
