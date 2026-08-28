# Traffic Observation Data Contract v1.0

| Field | Type | Rule |
|---|---|---|
| `event_id` | string | globally stable idempotency key |
| `trace_id` | string | end-to-end diagnostic correlation |
| `segment_id` | string | `SEG-\d{3}` |
| `road_name` | string | non-empty; public demo uses privacy-safe labels |
| `observed_at` | timestamptz | timezone required; event time |
| `ingested_at` | timestamptz | timezone required; ingestion time |
| `source_system` | enum | `TMAP`, `TCS`, `VDS`, `GPS` |
| `vehicle_type` | enum | `PASSENGER`, `BUS`, `TRUCK`, `ALL` |
| `segment_length_km` | double | `0 < x <= 100` |
| `speed_kph` | double | `0 < x <= 180` |
| `reference_speed_kph` | double | `0 < x <= 180` |
| `traffic_volume` | integer | `0 <= x <= 20,000` |
| `travel_time_seconds` | double | `0 < x <= 7,200` |
| `schema_version` | literal | `1.0` only |

## Failure policy

- Streaming JSON의 decode, missing field, type/range mismatch, unknown schema version은 retry가
  아니라 DLQ 대상입니다.
- File ingestion의 같은 오류는 `reject_reason`과 함께 quarantine Parquet에 남습니다.
- DLQ envelope는 error type/message와 source topic/partition/offset을 포함합니다.
- Transport retry로 같은 `event_id`가 여러 번 들어올 수 있으며 downstream이 이를 제거합니다.
- `ingested_at - observed_at > 15 minutes`이면 record를 버리지 않고 `is_late=true`로 보존합니다.

## File mapping

`mobility-ingest-file`의 column map은 canonical field를 key, source column을 value로 사용합니다.
mapping·원본 SHA-256·source system이 같으면 같은 ingest fingerprint가 생성됩니다.

```json
{
  "segment_id": "link_id",
  "observed_at": "measure_time",
  "speed_kph": "avg_speed",
  "traffic_volume": "volume_15m"
}
```

## Schema evolution

- backward-compatible optional field는 minor contract update로 추가합니다.
- required field/meaning/type 변경은 새 topic version과 병행 운영 후 consumer migration을 수행합니다.
- dbt `on_schema_change='fail'`은 조용한 schema drift를 차단합니다.
