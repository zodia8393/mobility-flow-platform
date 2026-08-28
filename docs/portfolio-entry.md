# Portfolio Entry — MobilityFlow DataOps

## Project title

**MobilityFlow DataOps — 교통 데이터 검수·적재·납품 운영 플랫폼**

기존 교통·공간 데이터 실무에서 반복했던 TMAP·TCS·VDS·GPS 원천 검수와 산출물 QA를
CSV/Parquet file ingestion, Kafka 수집, Spark 처리, Airflow orchestration, dbt quality gate,
납품 manifest까지 하나의 실제 사용 가능한 workflow로 구현했습니다.

### 기존 포트폴리오와의 연결

- **교통·공간 데이터 플랫폼:** 93,432,415행 audit, row·key·date·hash gate, 197 artifact/run을
  운영 대상과 기준선으로 사용
- **CatalogForge:** upload-before-commit, stable consumer group, DLQ, retry, idempotent replay 패턴 재사용
- **Legacy RDB Migration:** reconciliation, reject reason lineage, checkpoint, schema drift 차단 패턴 재사용
- **이번 확장:** AWS·Airflow·Spark·dbt·monitoring·delivery readiness를 실제 실행 가능한 제품으로 연결

## Resume bullets

- CSV·CSV.GZ·Parquet file에 source별 column mapping과 dry-run을 적용하고 valid row는 Bronze,
  invalid row는 오류 사유별 quarantine으로 분리하는 실사용 batch ingestion CLI 구현
- Redpanda/Kafka 3-partition feed에 schema contract와 DLQ를 적용하고, Parquet upload 이후에만
  offset을 commit하는 at-least-once ingestion 경로 구현
- Airflow 15분 DAG와 Spark `event_id` dedup, object manifest, PostgreSQL upsert를 결합해 retry와
  backfill 시 중복 적재를 방지하는 idempotent pipeline 설계
- dbt staging·incremental fact·정체/시간대 mart와 source freshness gate를 구성하고 Control
  Room·Prometheus·Grafana에서 freshness, DQ, job duration을 관측
- input=accepted+dedup reconciliation, dbt PASS, Silver SHA-256 manifest가 모두 통과할 때만
  산출물을 `READY`로 표시하는 delivery gate 구현
- Local failure drill에서 writer 중단 중 1,200개 event를 Kafka에 buffer한 뒤 17초 내 회수하고,
  invalid 66건 DLQ 격리와 즉시 replay `NO_DATA`를 확인 *(synthetic data, local[2] 기준)*
- Terraform으로 encryption/versioning이 적용된 AWS S3, immutable ECR, ECS/Fargate baseline,
  CloudWatch dashboard·alarm과 least-privilege task role을 코드화

## Interview walkthrough

1. **문제:** 서로 다른 교통 원천 파일을 반복 검수하고 산출물의 재사용·납품 가능 여부를 사람이
   row·hash·문서 단위로 확인해야 한다.
2. **파일 수집:** CSV/CSV.GZ/Parquet를 source mapping으로 정규화하고 dry-run 결과를 확인한 뒤
   valid/quarantine object로 분리한다.
3. **Feed 수집:** Kafka at-least-once를 전제로 payload를 contract 검증하고 invalid event는 DLQ로 보낸다.
4. **복구:** upload-before-commit, stable consumer group, event ID dedup으로 consumer crash를 견딘다.
5. **처리:** Spark가 new bronze object만 읽어 dedup, late-event flag, speed index를 계산한다.
6. **모델:** dbt가 event fact, road dimension, latest snapshot, congestion/action mart를 만든다.
7. **납품:** reconciliation·dbt·artifact hash가 모두 PASS일 때만 delivery manifest를 `READY`로 연다.
8. **운영:** Airflow retry/timeout/backfill, Prometheus/Grafana SLO, run manifest로 실패 원인과 재실행
   결과를 남긴다.
9. **Cloud:** storage adapter로 MinIO를 S3로 교체하고 Terraform/CloudWatch boundary를 사용한다.

## Claims boundary

- 공개 demo data는 실제 회사 원천이 아닌 deterministic synthetic traffic observation입니다.
- `93,432,415행`, `15.399GiB`, `197 artifact/run`은 기존 portfolio evidence ledger의 실무
  기준선이며 새 공개 demo의 처리량으로 재주장하지 않습니다.
- AWS infrastructure는 `terraform validate`와 account-backed `plan`까지 검증하며, 비용이 발생하는
  `apply`는 별도 승인 후 수행합니다.
- Spark benchmark는 single-host `local[2]` 결과이며 distributed-cluster 성능으로 표현하지 않습니다.
