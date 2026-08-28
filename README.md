# MobilityFlow

서울특별시의 실시간 도로소통 데이터를 주기적으로 수집하고, 원본 보존부터 품질검증과 게시 가능
판정까지 수행하는 운영형 데이터 파이프라인입니다.

![MobilityFlow live operation](docs/mobilityflow-live.gif)

> 실제 실행 전체 영상: [MP4 보기](docs/mobilityflow-live.mp4)

## 실제로 하는 일

- 서울 열린데이터광장 API에서 `광화문·덕수궁`, `강남 MICE 관광특구`, `여의도`의 도로 링크를
  15분마다 수집합니다.
- 수신한 원문을 gzip JSON으로 먼저 보존하고 SHA-256을 남깁니다.
- API의 `LINK_ID`, 속도, 도로명, 거리, 좌표열, 공식 정체 단계를 canonical Parquet로 변환합니다.
- Spark가 새 Bronze object만 읽어 `event_id` 중복을 제거하고 PostgreSQL에 멱등 적재합니다.
- dbt가 서울 원천 전용 freshness를 포함한 28개 data test와 source freshness를 통과한 경우에만 최신 mart를 공개합니다.
- row reconciliation과 Silver artifact hash까지 모두 통과해야 Control Room이 `READY`를 표시합니다.

API에 존재하지 않는 교통량이나 기준속도는 임의로 채우지 않습니다. 해당 필드는 `NULL`로 보존하고
정체 상태는 서울시가 제공한 `원활·서행·정체` 값을 그대로 표준화합니다.

## 현재 저장된 실제 데이터

`data/reference/seoul_traffic_latest.parquet`는 서울시 API에서 직접 수집한 공개 데이터 Snapshot입니다.

| 항목 | 값 |
|---|---:|
| 수집 시각 | 2026-08-28 18:22 KST |
| 지역 | 3개 |
| 도로 링크 | 455개 |
| 검증 통과 | 455개 |
| 격리 | 0개 |

원천별 payload hash와 수집 조건은
[`data/reference/source_manifest.json`](data/reference/source_manifest.json)에 기록되어 있습니다.

데이터 제공처는 [서울시 실시간 도시데이터](https://data.seoul.go.kr/SeoulRtd/)이며,
[서울 열린데이터광장 데이터셋](https://data.seoul.go.kr/dataList/OA-21285/A/1/datasetView.do)의
이용정책과 출처표시 조건을 따릅니다. 서울시는 도로소통 데이터의 갱신주기를 5분으로 안내합니다.

## 처리 흐름

![MobilityFlow architecture](docs/architecture.svg)

```text
Seoul Real-Time City Data API
  → Raw Landing JSON.gz + SHA-256
  → Canonical Bronze Parquet
  → Airflow 15-minute schedule
  → Spark dedup / late-event handling
  → PostgreSQL + dbt marts / 28 tests / source freshness
  → READY or BLOCKED manifest
  → Control Room + Prometheus + Grafana
```

각 source response와 Bronze/Silver artifact는 run 단위로 추적할 수 있습니다. 같은 5분 Snapshot을
다시 받아도 `area_code + LINK_ID + snapshot_at` 기반 `event_id`가 같아 중복 적재되지 않습니다.

## 실행

Requirements: Docker 24+, Docker Compose v2+, 약 8 GB memory, 서울 열린데이터광장 인증키.

```bash
cp .env.example .env
# .env의 SEOUL_OPEN_DATA_API_KEY에 무료 발급 키 입력
make live
```

`make live`는 전체 서비스를 기동하고 실제 API 수집 → Spark → dbt → 게시 Gate까지 Airflow DAG로
실행합니다.

| 화면 | URL | 용도 |
|---|---|---|
| Control Room | http://localhost:18000 | 실제 도로 링크, 정체, freshness, 게시 Gate |
| Airflow | http://localhost:18080 | 수집·변환·품질검증 task와 retry 이력 |
| Grafana | http://localhost:13000 | 처리량, 실패, 실행시간, freshness |
| MinIO | http://localhost:19001 | Raw/Bronze/Silver object와 partition |
| Spark UI | http://localhost:14040 | 실행 중 stage와 task |

현재 설정으로 한 번만 다시 수집하려면 다음을 실행합니다.

```bash
make sync
```

실제 Control Room을 같은 방식으로 다시 녹화하려면 서비스 실행 후 `make capture`를 실행합니다.

Object store에 적재하지 않고 공식 API 응답만 검증·보관할 수도 있습니다.

```bash
uv run mobility-seoul-sync \
  --no-upload \
  --snapshot-output data/reference/seoul_traffic_latest.parquet \
  --report data/reference/source_manifest.json
```

## 운영 안전장치

- **Source preservation:** 필요한 도로소통 응답을 변환 전에 gzip JSON으로 보존
- **Idempotency:** 5분 Snapshot 단위 deterministic `event_id`, object manifest, PostgreSQL upsert
- **Quarantine:** 좌표·속도·거리·schema 오류를 원천 링크와 사유가 포함된 object로 격리
- **Quality gate:** `unique`, `not_null`, `relationships`, `accepted_values`, reconciliation SQL
- **Freshness:** warning 15분, failure 30분
- **Publishing gate:** source object, row reconciliation, dbt PASS, artifact SHA-256가 모두 필요
- **Recovery:** Airflow retry/timeout, 새 object만 처리, 동일 run replay 안전성
- **Observability:** Prometheus target과 provisioned Grafana dashboard

운영 대응과 replay 절차는 [Operations runbook](docs/runbook.md), field 정의와 원천 매핑은
[Data contract](docs/data-contract.md)에 있습니다.

## Cloud 배포 경계

Local MinIO와 AWS S3는 같은 storage adapter를 사용합니다. Terraform은 versioning/encryption이
적용된 S3, ECR, ECS, CloudWatch dashboard·alarm, SNS와 least-privilege IAM을 선언합니다.

```bash
cd infra/terraform
terraform init
terraform plan -var='alert_email=you@example.com'
```

실제 AWS `apply`는 비용과 외부 상태를 만들기 때문에 자동 실행하지 않습니다. 자세한 내용은
[AWS deployment guide](docs/aws-deployment.md)에 있습니다.

## 검증

```bash
make check
make failure-drill
```

- Unit/contract test 27건
- dbt data test 28건(서울 원천 전용 freshness 포함) + source freshness
- Terraform fmt/init/validate
- Docker Compose validation
- GitHub Actions quality gate

## Repository map

```text
airflow/dags/              official API sync → Spark → dbt orchestration
data/reference/            attributed real public-data Snapshot and manifest
dbt/                       staging, incremental fact, marts and tests
infra/terraform/           AWS deployment baseline
monitoring/                Prometheus and Grafana provisioning
src/mobility_flow/         live connector, ingestion, Spark runner and API
docs/                      contracts, runbook, architecture and execution video
tests/                     deterministic contract and failure tests
```

## 데이터 사용 범위

- 저장된 도로 데이터는 서울특별시가 공개한 집계형 도로소통 정보이며 회사 내부 원천을 포함하지 않습니다.
- `observed_at`은 API에 별도 측정시각이 없어 수집시각을 5분 단위로 내린 Snapshot 기준시각입니다.
- Terraform은 S3·ECR·ECS·CloudWatch·IAM resource를 선언하지만 기본 설정에서는 AWS resource를
  자동 생성하지 않습니다.

License: MIT. 외부 데이터에는 원 제공기관의 이용조건이 우선 적용됩니다.
