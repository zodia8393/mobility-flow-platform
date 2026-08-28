select event_id
from {{ ref('stg_traffic_observation') }}
where speed_kph <= 0
   or reference_speed_kph <= 0
   or segment_length_km <= 0
   or traffic_volume < 0
   or travel_time_seconds <= 0
