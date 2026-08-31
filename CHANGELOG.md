# Changelog

## 0.1.0 · 2026-08-31

- 서울 실시간 도시데이터 API 3개 지역 수집과 공개 Snapshot을 기준선으로 고정
- raw JSON.gz·SHA-256 보존, canonical Parquet, PostgreSQL/dbt mart 연결
- Airflow schedule·manual run·backfill과 READY/BLOCKED publishing gate 검증
- 장애복구 drill에서 backlog 회수, DLQ 격리, idempotent replay 확인
- PySpark 사용 범위를 `local[2]` 변환 정확성 검증으로 명시
- local Airflow schedule 누적 115,570행·최근 10/10 성공 Snapshot과 실행 영상을 갱신
- 계정·credential·내부 경로를 공개 문서와 evidence에서 제외
