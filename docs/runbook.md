# MobilityFlow DataOps Operations Runbook

## SLO and alert policy

| Signal | Target | Warning | Failure | First response |
|---|---:|---:|---:|---|
| Warehouse freshness | ≤ 15 min | > 15 min | > 30 min | Kafka lag → bronze upload → Airflow run 순서로 확인 |
| File reconciliation | input = accepted + rejected | - | mismatch | ingest report와 quarantine object 확인 |
| dbt quality gate | 100% pass | - | any failure | failed test SQL과 offending rows 확인 |
| Delivery readiness | `READY` | - | `BLOCKED` | row reconciliation → dbt → artifact hash 순서로 확인 |
| Bronze upload | continuous | 1 retry | repeated failure | object-store health와 credential chain 확인 |
| Spark job | ≤ 12 min | > 8 min | timeout 12 min | input object 수, skew, executor log 확인 |

## Triage order

1. Control Room의 delivery `READY/BLOCKED`, last run, freshness, service status를 확인합니다.
2. Airflow Grid에서 최초 실패 task와 retry 횟수를 확인합니다.
3. Kafka/Redpanda consumer group lag를 확인합니다.
4. MinIO/S3의 마지막 bronze object timestamp를 확인합니다.
5. Spark run의 input/output row와 duplicate/late count를 대조합니다.
6. dbt test artifact에서 실패 SQL과 row를 확인합니다.
7. delivery manifest의 output object count, row count, SHA-256를 확인합니다.

## File preflight

운영 object store에 쓰기 전에 `--dry-run`으로 schema와 row reconciliation을 확인합니다.

```bash
uv run mobility-ingest-file /path/to/source.csv.gz \
  --source-system TMAP \
  --column-map @/path/to/column_map.json \
  --dry-run
```

`accepted_rows + rejected_rows = input_rows`가 아니거나 예상하지 못한 reject reason이 있으면 적재를
중단합니다. source 담당자가 column 의미·단위·timezone을 확인한 뒤에만 `--dry-run`을 제거합니다.

## Safe replay

```bash
docker compose exec airflow-scheduler airflow dags trigger mobility_flow_15m
```

- Bronze Writer는 object upload가 끝난 뒤에만 Kafka offset을 commit합니다.
- Spark는 `ops.object_manifest`에 없는 object만 처리합니다.
- replay 중복은 `event_id`와 PostgreSQL `ON CONFLICT`로 제거합니다.
- file ingest는 source SHA-256와 mapping을 결합한 fingerprint를 object key에 사용합니다.
- 이미 성공한 run을 강제로 재처리할 때만 Spark API의 `force_reprocess=true`를 사용합니다.

## Backfill

기본 DAG는 `catchup=false`입니다. 의도하지 않은 대량 실행을 막기 위해 기간 backfill은 명시적으로
실행합니다.

```bash
docker compose exec airflow-scheduler airflow dags backfill mobility_flow_15m \
  --start-date 2026-08-27T00:00:00 \
  --end-date 2026-08-27T23:59:59
```

실행 전 대상 prefix의 object 수와 예상 Spark input 크기를 확인합니다.

## Failure drill

```bash
make failure-drill
```

Drill은 다음을 자동 검증합니다.

1. Bronze Writer를 중지한 상태에서 Kafka에 event를 적재합니다.
2. Writer 재기동 후 stable consumer group이 backlog를 회수하는지 확인합니다.
3. invalid event가 DLQ로 분리되는지 확인합니다.
4. Spark job을 연속 두 번 실행해 두 번째 실행이 `NO_DATA`인지 확인합니다.
5. 결과와 recovery time을 `docs/evidence/generated/failure_drill.json`에 기록합니다.

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
