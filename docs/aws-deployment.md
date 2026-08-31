# AWS Deployment Guide

## Scope

Terraform baseline은 다음 resource를 만듭니다.

- versioning, AES-256 encryption, public access block, lifecycle이 적용된 S3 data lake
- scan-on-push와 immutable tag가 적용된 ECR repositories
- Container Insights가 활성화된 ECS cluster와 Fargate/Fargate Spot capacity providers
- least-privilege S3/CloudWatch pipeline task role
- CloudWatch log group, dashboard, freshness·DQ alarms, optional SNS email

RDS, MSK, always-on ECS service는 default로 만들지 않습니다. 개발·검증 단계의 상시 비용을
피하면서 실제 S3 integration과 operations telemetry를 확인하기 위한 범위입니다.

## Deployed baseline · 2026-08-31

`ap-northeast-2`의 `dev` environment에 baseline을 적용했습니다. 실제 서울 API 455행의 raw·Bronze
object를 S3 adapter로 적재하고 local pipeline의 warehouse·freshness·DQ·duration metric을 CloudWatch에
게시했습니다. Terraform post-apply plan은 drift 0이었습니다.

초기 ECR scan의 critical finding으로 task promotion을 차단한 뒤 배포 application image를 digest-pinned
Wolfi base로 교체했습니다. ECR native scan 4/4의 OS package finding과 배포 app의 Trivy critical/high는
모두 0건입니다. Fargate one-shot task는 ECR pull, S3 object byte-read, CloudWatch metric·log publish를
exit code 0으로 완료했습니다. 상시 ECS service는 만들지 않았고 기존 EC2 instance도 변경하지 않았습니다.
공개 가능한 검증값은
[AWS deployment evidence](evidence/aws_deployment_20260831.json)에 있습니다.

## Preflight

```bash
aws sts get-caller-identity
aws configure get region
terraform fmt -check -recursive
terraform init
terraform validate
terraform plan -out mobility-flow.tfplan
```

`terraform plan`에서 생성 resource와 예상 region을 검토한 뒤에만 apply합니다.

## Apply

```bash
terraform apply mobility-flow.tfplan
export S3_BUCKET="$(terraform output -raw lake_bucket_name)"
export S3_ENDPOINT_URL=""
export AWS_REGION="ap-northeast-2"
```

Application은 static access key를 요구하지 않습니다. Local AWS profile, assumed role, ECS task role 등
AWS default credential chain을 사용합니다. `.env`나 Git에 credential을 저장하지 않습니다.

ECR native scan 대상은 single-platform manifest여야 하므로 cloud image는 `docker build --provenance=false`로
생성합니다. 배포 application image는 CI에서도 Trivy의 OS·library `CRITICAL,HIGH` finding을 차단합니다.
PySpark image는 bundled upstream JAR finding이 남아 있어 AWS runtime으로 승격하지 않습니다.

## Hybrid verification

Airflow·Kafka·PySpark는 local container에서 실행하되 bronze/silver object는 실제 S3 bucket에 적재할 수
있습니다. 다음 증거를 남깁니다.

1. Terraform plan/apply output의 resource count
2. S3 object의 prefix, size, encryption, versioning 상태
3. CloudWatch custom metric과 alarm state
4. 동일 run의 Airflow log, DB row count, evidence JSON

현재 hybrid 실행은 Airflow·PostgreSQL/dbt·PySpark를 local Docker에서 수행하고, 측정된 raw/Bronze
artifact와 metric·log를 AWS에 게시합니다. Fargate runtime check는 ECR pull·task role 기반 S3 byte-read와
CloudWatch publish만 검증하며, 상시 application service에는 managed database·private network·load
balancer 설계가 추가로 필요합니다.

Account ID, access key, secret, internal endpoint는 screenshot과 공유 artifact에서 제거합니다.

## Cost and teardown

- Default 구성에는 NAT Gateway, RDS, MSK, MWAA, always-on Fargate task가 없습니다.
- ECR image와 S3 data volume이 커지면 저장 비용이 발생합니다.
- SNS email subscription은 확인 전까지 `PendingConfirmation` 상태입니다.
- Resource 삭제는 state와 대상 account/region을 확인한 뒤 별도 승인 하에 `terraform destroy`를
  수행합니다. 이 repository의 automation은 자동 destroy를 실행하지 않습니다.
