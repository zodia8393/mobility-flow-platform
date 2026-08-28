# Evidence Guide

## Verified local run — 2026-08-28 KST

운영 demo 수치는 Docker local environment, Spark `local[2]`, privacy-safe synthetic traffic
observation을 사용한 실제 실행 결과입니다. 기존 포트폴리오 실무 기준선과 합산하지 않으며 Cloud
또는 distributed-cluster benchmark로 해석하지 않습니다.

| Verification | Result |
|---|---:|
| File preflight | 8 input / 8 accepted / 0 rejected |
| File → Bronze → Spark → dbt delivery | `READY`, 4/4 gates PASS, 3 SHA-256 artifacts |
| Stream E2E input after schema quarantine | 984 rows |
| Spark deduplication | 42 duplicates removed |
| Accepted warehouse rows in the stream run | 942 rows |
| Late events (>15 min) | 43 rows |
| Spark transform duration | 3.867 sec |
| dbt data tests | 27/27 PASS |
| dbt source freshness | PASS |
| Failure drill buffered events | 1,200 messages |
| Failure drill recovery | 17 sec |
| Failure drill DLQ | 56 invalid events |
| Immediate replay | `NO_DATA` (idempotent) |

Machine-readable samples:

- [example_run_summary.json](example_run_summary.json)
- [example_file_ingest.json](example_file_ingest.json)
- [example_failure_drill.json](example_failure_drill.json)

`make demo`와 `make failure-drill`을 실행하면 current evidence가 `generated/` 아래에 새로 생성됩니다.
Generated result는 환경마다 달라지므로 Git에서 제외합니다.

## Screenshot checklist

- Control Room 상단의 `SYNTHETIC DATA` 범위 표시
- Kafka, object store, PostgreSQL service health
- freshness, row count, DQ/dbt status
- road-segment congestion map and source/action queue
- delivery `READY/BLOCKED`, row reconciliation, dbt, SHA-256 artifact gates
- run-level input/accepted/dedup/late/duration
- `SUCCESS`, `TRANSFORMED`, `NO_DATA`, `FAILED` state evidence
