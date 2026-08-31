# MobilityFlow DataOps Operations Runbook

## SLO and alert policy

| Signal | Target | Warning | Failure | First response |
|---|---:|---:|---:|---|
| Warehouse freshness | ≤ 15 min | > 15 min | > 30 min | 서울 API sync → Bronze → Airflow run 순서로 확인 |
| File reconciliation | input = accepted + rejected | - | mismatch | ingest report와 quarantine object 확인 |
| dbt quality gate | 100% pass | - | any failure | failed test SQL과 offending rows 확인 |
| Seoul source freshness | ≤ 15 min | > 15 min | > 30 min | API key·quota·upstream status 확인 |
| Delivery readiness | `READY` | - | `BLOCKED` | row reconciliation → dbt → artifact hash 순서로 확인 |
| Seoul API sync | every 15 min | 1 missed run | > 30 min | key·quota·upstream status 확인 |
| Bronze upload | each successful sync | 1 retry | repeated failure | object-store health와 credential chain 확인 |
| PySpark local job | ≤ 12 min | > 8 min | timeout 12 min | input object 수와 executor log 확인 |

## Triage order

1. Control Room의 delivery `READY/BLOCKED`, last run, freshness, service status를 확인합니다.
2. Airflow Grid에서 최초 실패 task와 retry 횟수를 확인합니다.
3. `/api/source`에서 마지막 서울 API Snapshot과 지역별 payload hash를 확인합니다.
4. MinIO/S3의 마지막 landing·bronze object timestamp를 확인합니다.
5. PySpark local run의 input/output row와 duplicate/late count를 대조합니다.
6. dbt test artifact에서 실패 SQL과 row를 확인합니다.
7. delivery manifest의 output object count, row count, SHA-256를 확인합니다.

## Live source check

Object store에 적재하지 않고 서울 API 응답과 contract만 확인할 수 있습니다.

```bash
uv run mobility-seoul-sync --no-upload
```

`accepted_rows + rejected_rows = input_rows`를 확인하고 reject가 있으면 원천 링크 ID와 사유를
확인합니다. API key와 key가 포함된 요청 URL은 report나 log에 남기지 않습니다.

## Safe replay

```bash
docker compose exec airflow-scheduler airflow dags trigger mobility_flow_15m
```

- 서울 connector는 source response landing upload 후 canonical Bronze를 생성합니다.
- PySpark local 변환은 `ops.object_manifest`에 없는 object만 처리합니다.
- replay 중복은 `event_id`와 PostgreSQL `ON CONFLICT`로 제거합니다.
- 서울 source payload SHA-256와 5분 Snapshot이 object key와 event ID에 반영됩니다.
- 이미 성공한 run을 강제로 재처리할 때만 PySpark API의 `force_reprocess=true`를 사용합니다.

## Backfill

기본 DAG는 `catchup=false`이며 과거 logical date로 실행해도 실시간 API의 현재 Snapshot만 반환합니다.
따라서 과거 backfill은 API를 재호출하지 않고 보존한 landing/Bronze object를 대상으로 수행합니다.

```bash
curl --fail --request POST http://localhost:18081/jobs/bronze-to-silver \
  --header 'Content-Type: application/json' \
  --data '{"prefix":"bronze/seoul_citydata/","force_reprocess":true}'
```

실행 전 대상 prefix와 `ops.object_manifest`를 확인하고 별도 run ID로 결과를 검증합니다.

## Failure drill

```bash
make failure-drill
```

Drill은 다음을 자동 검증합니다.

1. Bronze Writer를 중지한 상태에서 Kafka에 event를 적재합니다.
2. Writer 재기동 후 stable consumer group이 backlog를 회수하는지 확인합니다.
3. invalid event가 DLQ로 분리되는지 확인합니다.
4. PySpark local job을 연속 두 번 실행해 두 번째 실행이 `NO_DATA`인지 확인합니다.
5. 결과와 recovery time을 runtime evidence artifact로 기록합니다.

## Delivery decision

`GET /api/delivery/latest`는 다음 조건이 모두 PASS일 때만 `READY`를 반환합니다.

1. 처리한 source object가 1개 이상 존재
2. `input_rows = accepted_rows + duplicates_removed`
3. dbt build·test·freshness PASS
4. 모든 Silver artifact의 SHA-256 manifest 존재

`READY`는 기술적 품질 gate이며 계약·정책·담당자 승인 자체를 자동화하지 않습니다.

## Rollback

- Application rollback: immutable image tag로 이전 container image를 재배포합니다.
- Data rollback: S3 versioning을 사용하고 silver run prefix 단위로 consumer view를 되돌립니다.
- Warehouse rollback: dbt artifact와 Git SHA를 기준으로 이전 model version을 build합니다.
- `docker compose down`은 volume을 삭제하지 않습니다. Volume 삭제는 별도 승인·backup 후 수행합니다.
