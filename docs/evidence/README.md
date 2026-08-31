# Runtime Evidence

## 실제 공개 원천

- 제공기관: 서울특별시·서울 열린데이터광장
- 데이터셋: 서울시 실시간 도시데이터·도로소통
- 수집 방식: API key를 환경변수로 주입한 live request
- 원천 보존: 도로소통 response gzip JSON + payload SHA-256
- 변환 결과: [공개 Snapshot](../../data/reference/seoul_traffic_latest.parquet)
- 수집 근거: [Manifest](../../data/reference/source_manifest.json)

Reference Snapshot은 광화문·덕수궁, 강남 MICE 관광특구, 여의도 3개 지역의 실제 도로 링크
455개를 포함하며 455개 모두 contract 검증을 통과했습니다. API에 없는 기준속도와 교통량은
생성하지 않고 `NULL`로 보존합니다.

## End-to-end 확인 항목

1. Airflow의 `sync_seoul_live_traffic` task가 공식 API를 호출합니다.
2. landing JSON과 canonical Bronze Parquet의 object key가 run에 남습니다.
3. Spark input·accepted·duplicate 수가 reconciliation을 만족합니다.
4. 서울 원천 전용 freshness를 포함한 dbt data test 28개와 source freshness가 통과합니다.
5. Silver artifact별 row count, byte size, SHA-256가 manifest에 남습니다.
6. Control Room의 source 기준시각·지역·도로 링크와 warehouse 값이 일치합니다.
7. source object, reconciliation, dbt, artifact hash가 모두 PASS일 때만 `READY`입니다.

`make live` 실행 결과는 환경·시각에 따라 달라지는 runtime artifact로 생성되며 Git에서 제외합니다.
공개 저장소의 GIF/MP4는 같은 live run의 Control Room을 녹화한
것이며 정적 mockup이 아닙니다.

## 별도 장애복구 검증

`failure_drill_20260828.json`은 live source 값이 아니라 pipeline transport의 장애복구 시험 결과입니다.
Bronze writer 중단 중 Kafka backlog 보존, invalid DLQ 격리, immediate replay `NO_DATA`를 검증합니다.
이 결과를 서울시 live traffic 수치와 합산하지 않습니다.
