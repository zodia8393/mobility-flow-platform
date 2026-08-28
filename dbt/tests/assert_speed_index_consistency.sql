select event_id
from {{ ref('stg_traffic_observation') }}
where reference_speed_kph is not null
  and abs(speed_index - (speed_kph / reference_speed_kph)) > 0.00001
