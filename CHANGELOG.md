# Changelog

## 0.1.1 · 2026-08-31

- AWS S3·ECR·ECS cluster·CloudWatch·IAM baseline 25개 resource 실제 배포
- 실제 서울 API 455행을 AWS S3 raw 3개·Bronze 3개 object로 적재
- CloudWatch metric 4개와 freshness·DQ alarm, deployment log 검증
- CloudWatch dashboard·alarm에 `Environment` dimension을 일치시켜 metric 누락 수정
- ECR scan 4/4 완료 후 critical finding을 확인해 ECS runtime promotion 차단
- 계정·bucket·repository URL·ARN을 제거한 deployment evidence 추가

## 0.1.0 · 2026-08-31

- 서울 실시간 도시데이터 API 3개 지역 수집과 공개 Snapshot을 기준선으로 고정
- raw JSON.gz·SHA-256 보존, canonical Parquet, PostgreSQL/dbt mart 연결
- Airflow schedule·manual run·backfill과 READY/BLOCKED publishing gate 검증
- 장애복구 drill에서 backlog 회수, DLQ 격리, idempotent replay 확인
- PySpark 사용 범위를 `local[2]` 변환 정확성 검증으로 명시
- local Airflow schedule 누적 115,570행·최근 10/10 성공 Snapshot과 실행 영상을 갱신
- 계정·credential·내부 경로를 공개 문서와 evidence에서 제외
