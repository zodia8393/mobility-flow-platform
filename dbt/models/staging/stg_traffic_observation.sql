with source as (
    select * from {{ source('raw', 'traffic_observation') }}
),

renamed as (
    select
        event_id,
        trace_id,
        segment_id,
        road_name,
        latitude,
        longitude,
        observed_at,
        ingested_at,
        source_system,
        vehicle_type,
        segment_length_km,
        speed_kph,
        reference_speed_kph,
        traffic_volume,
        travel_time_seconds,
        speed_index,
        congestion_level,
        is_late,
        schema_version,
        loaded_at
    from source
)

select * from renamed
