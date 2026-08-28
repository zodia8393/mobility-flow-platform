select event_id
from {{ ref('stg_traffic_observation') }}
where abs(speed_index - (speed_kph / reference_speed_kph)) > 0.00001
