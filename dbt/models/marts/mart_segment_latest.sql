with ranked as (
    select
        *,
        row_number() over (partition by segment_id order by observed_at desc, ingested_at desc) as recency_rank
    from {{ ref('fct_traffic_observation') }}
)

select
    segment_id,
    observed_at,
    source_system,
    speed_kph,
    reference_speed_kph,
    traffic_volume,
    travel_time_seconds,
    speed_index,
    congestion_level,
    is_late
from ranked
where recency_rank = 1
