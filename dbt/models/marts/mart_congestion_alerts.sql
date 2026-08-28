select
    latest.segment_id,
    segment.road_name,
    segment.area_code,
    segment.area_name,
    segment.latitude,
    segment.longitude,
    segment.start_latitude,
    segment.start_longitude,
    segment.end_latitude,
    segment.end_longitude,
    latest.observed_at,
    latest.source_system,
    latest.speed_kph,
    latest.reference_speed_kph,
    latest.traffic_volume,
    latest.speed_index,
    latest.congestion_level,
    case
        when latest.congestion_level = 'SEVERE' then 'VERIFY_SOURCE_AND_AGGREGATION'
        when latest.congestion_level = 'CONGESTED' then 'REVIEW_CONGESTION'
        when latest.congestion_level = 'SLOW' then 'MONITOR_TREND'
        else 'NONE'
    end as recommended_action,
    case latest.congestion_level
        when 'SEVERE' then 1
        when 'CONGESTED' then 2
        when 'SLOW' then 3
        else 3
    end as action_priority
from {{ ref('mart_segment_latest') }} latest
join {{ ref('dim_road_segment') }} segment using (segment_id)
where latest.congestion_level <> 'SMOOTH'
