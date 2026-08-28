select
    date_trunc('hour', observed_at) as observed_hour,
    count(*) as observations,
    count(distinct segment_id) as active_segments,
    avg(speed_kph) as average_speed_kph,
    avg(speed_index) as average_speed_index,
    avg(case when congestion_level = 'SMOOTH' then 1.0 else 0.0 end) as smooth_observation_rate,
    sum(case when congestion_level = 'SEVERE' then 1 else 0 end) as severe_observations,
    sum(case when is_late then 1 else 0 end) as late_observations
from {{ ref('fct_traffic_observation') }}
group by 1
