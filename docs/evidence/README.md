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
3. PySpark `local[2]` input·accepted·duplicate 수가 reconciliation을 만족합니다.
4. 서울 원천 전용 freshness를 포함한 dbt data test 28개와 source freshness가 통과합니다.
5. Silver artifact별 row count, byte size, SHA-256가 manifest에 남습니다.
6. Control Room의 source 기준시각·지역·도로 링크와 warehouse 값이 일치합니다.
7. source object, reconciliation, dbt, artifact hash가 모두 PASS일 때만 `READY`입니다.

`make live` 실행 결과는 환경·시각에 따라 달라지는 runtime artifact로 생성되며 Git에서 제외합니다.
공개 저장소의 GIF/MP4는 같은 live run의 Control Room을 녹화한
것이며 정적 mockup이 아닙니다.

## Scheduled operation snapshot · 2026-08-31

- Warehouse 누적 관측: 115,570행
- 최근 실행: 10/10 `SUCCESS`, dbt `PASS` 10/10
- 최신 run: 455 input = 455 accepted + 0 duplicate
- 게시 판정: `READY`, gate 4/4 `PASS`

원시 계측값은 [scheduled operation evidence](scheduled_operation_20260831.json)에 남겼습니다. 이는
local Docker Compose에서 Airflow 15분 schedule을 유지한 결과이며, managed Airflow·분산 Spark·SLA
운영 성과를 의미하지 않습니다.

## AWS hybrid deployment · 2026-08-31

- Terraform: 25 added, 0 changed, 0 destroyed; post-apply drift 0
- S3: 실제 서울 API raw 3개·Bronze 3개를 포함한 13 objects, versioning·AES-256·public block 4/4
- ECR: repository 4개, native scan 4/4 OS package finding 0, 배포 app Trivy critical/high 0
- CloudWatch: 실제 pipeline metric 5개, Fargate log, freshness·DQ alarm `OK`
- ECS: Fargate one-shot runtime check 4회 exit code 0

초기 base image의 critical finding으로 runtime promotion을 중단한 뒤 배포 app을 Wolfi base로 재구축해
Trivy critical/high 0건을 확인했습니다. Fargate task는 ECR image를 pull하고 S3 object의 첫 byte를 읽은 뒤
`CloudRuntimeHealthy` metric을 게시했습니다. PySpark image는 upstream JAR finding이 남아 있어 local 검증
범위로 고정했습니다. Airflow·dbt·PySpark runtime 또는 always-on service를 AWS에서 운영했다는 주장은
아닙니다. 계정·bucket·repository URL·ARN을 제거한 계측값은
[sanitized evidence](aws_deployment_20260831.json)에 있습니다.

## 별도 장애복구 검증

`failure_drill_20260828.json`은 live source 값이 아니라 pipeline transport의 장애복구 시험 결과입니다.
Bronze writer 중단 중 Kafka backlog 보존, invalid DLQ 격리, immediate replay `NO_DATA`를 검증합니다.
이 결과를 서울시 live traffic 수치와 합산하지 않습니다.
