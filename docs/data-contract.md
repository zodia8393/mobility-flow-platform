# Traffic Observation Data Contract v1.1

## 서울시 실시간 도시데이터 매핑

| Canonical field | 서울시 원천 | Rule |
|---|---|---|
| `event_id` | derived | `area_code + LINK_ID + 5-minute snapshot_at` SHA-256 |
| `trace_id` | derived | source payload SHA-256 prefix |
| `segment_id` | `LINK_ID` | non-empty stable road link id |
| `road_name` | `ROAD_NM` | non-empty |
| `area_code`, `area_name` | `AREA_CD`, `AREA_NM` | API service area |
| `start_*`, `end_*` | `XYLIST` | first/last WGS84 point |
| `latitude`, `longitude` | `XYLIST` | polyline centroid for display/query |
| `observed_at` | derived | retrieval time floored to official 5-minute interval |
| `ingested_at` | derived | API retrieval time in UTC |
| `source_system` | fixed | `SEOUL_TOPIS` |
| `segment_length_km` | `DIST` | meter → kilometer, `0 < x <= 100` |
| `speed_kph` | `SPD` | `0 < x <= 180` |
| `travel_time_seconds` | derived | `length_km / speed_kph × 3,600` |
| `source_congestion_level` | `IDX` | 원활→SMOOTH, 서행→SLOW, 정체→CONGESTED |
| `reference_speed_kph` | not provided | `NULL`; 추정값을 만들지 않음 |
| `traffic_volume` | not provided | `NULL`; 추정값을 만들지 않음 |
| `source_payload_sha256` | derived | relevant source response SHA-256 |
| `schema_version` | fixed | `1.1` |

## 원천 보존과 시간 의미

- API 응답 중 도로소통 영역을 변환 전에 `landing/seoul_citydata/`에 gzip JSON으로 보존합니다.
- 서울시 응답에는 도로 링크별 별도 측정시각이 없으므로 `observed_at`은 retrieval time을 공식 갱신주기인
  5분 단위로 내린 Snapshot 기준시각입니다.
- 동일 Snapshot의 재수집은 같은 `event_id`를 만들며 PySpark `local[2]`와 PostgreSQL에서 중복을 제거합니다.
- `ingested_at - observed_at > 15 minutes`인 record는 삭제하지 않고 `is_late=true`로 보존합니다.

## 오류 처리

- 응답 구조, 좌표 범위, 속도, 거리, 필수 field가 contract를 위반하면 해당 링크를 quarantine에
  원천 ID와 사유와 함께 저장합니다.
- API timeout과 5xx는 최대 3회 exponential backoff 후 Airflow retry 대상으로 넘깁니다.
- API key는 환경변수로만 주입하며 URL·manifest·log·object에 기록하지 않습니다.

## 다른 교통 원천

`mobility-ingest-file`은 허가된 CSV·CSV.GZ·Parquet 원천에 column mapping을 적용할 수 있습니다.
기존 `TMAP`, `TCS`, `VDS`, `GPS` contract v1.0은 호환 입력으로 유지하며 source-specific field가 없는
경우 해당 값은 `NULL`로 보존합니다.

## Schema evolution

- optional field 추가는 minor version으로 올리고 dbt incremental model은 `sync_all_columns`로 반영합니다.
- grain, required field, field 의미 변경은 major version과 별도 Bronze prefix를 사용합니다.
- source field를 추정값으로 채우지 않으며 derived field는 계산식과 provenance를 문서화합니다.
