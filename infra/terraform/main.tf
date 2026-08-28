locals {
  name = "${var.project_name}-${var.environment}"
  bucket_name = var.bucket_name != "" ? var.bucket_name : format(
    "%s-%s-%s",
    local.name,
    data.aws_caller_identity.current.account_id,
    var.aws_region
  )
  repositories = toset(["api", "bronze-writer", "producer", "spark-runner"])
}

resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "cost-and-recovery-guardrails"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_ecr_repository" "service" {
  for_each = local.repositories

  name                 = "${local.name}/${each.key}"
  image_tag_mutability = "IMMUTABLE"

  encryption_configuration {
    encryption_type = "AES256"
  }

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "service" {
  for_each   = aws_ecr_repository.service
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the latest 10 immutable images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecs_cluster" "platform" {
  name = local.name
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "platform" {
  cluster_name       = aws_ecs_cluster.platform.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }
}

resource "aws_cloudwatch_log_group" "platform" {
  name              = "/mobility-flow/${var.environment}"
  retention_in_days = var.log_retention_days
}

resource "aws_sns_topic" "operations" {
  name = "${local.name}-operations"
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alert_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.operations.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_metric_alarm" "freshness" {
  alarm_name          = "${local.name}-freshness-over-30m"
  alarm_description   = "No fresh warehouse event for more than 30 minutes."
  namespace           = "MobilityFlow/Pipeline"
  metric_name         = "FreshnessSeconds"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 1800
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.operations.arn]
  ok_actions          = [aws_sns_topic.operations.arn]
}

resource "aws_cloudwatch_metric_alarm" "quality" {
  alarm_name          = "${local.name}-data-quality-failure"
  alarm_description   = "At least one dbt or contract quality check failed."
  namespace           = "MobilityFlow/Pipeline"
  metric_name         = "DataQualityFailures"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.operations.arn]
}

resource "aws_cloudwatch_dashboard" "operations" {
  dashboard_name = "${local.name}-operations"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 8, height = 6,
        properties = {
          title   = "Pipeline freshness", region = var.aws_region, view = "singleValue",
          metrics = [["MobilityFlow/Pipeline", "FreshnessSeconds"]], period = 300, stat = "Maximum"
        }
      },
      {
        type = "metric", x = 8, y = 0, width = 8, height = 6,
        properties = {
          title   = "Data quality failures", region = var.aws_region, view = "singleValue",
          metrics = [["MobilityFlow/Pipeline", "DataQualityFailures"]], period = 300, stat = "Sum"
        }
      },
      {
        type = "metric", x = 16, y = 0, width = 8, height = 6,
        properties = {
          title   = "Job duration", region = var.aws_region,
          metrics = [["MobilityFlow/Pipeline", "JobDurationSeconds"]], period = 300, stat = "p95"
        }
      },
      {
        type = "log", x = 0, y = 6, width = 24, height = 8,
        properties = {
          title = "Recent pipeline errors", region = var.aws_region,
          query = "SOURCE '${aws_cloudwatch_log_group.platform.name}' | fields @timestamp, service, run_id, @message | filter level = 'ERROR' | sort @timestamp desc | limit 50"
        }
      }
    ]
  })
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "${local.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "pipeline_task" {
  name               = "${local.name}-pipeline-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "pipeline_task" {
  statement {
    sid = "LakeObjectAccess"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*"
    ]
  }

  statement {
    sid       = "PublishPipelineMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["MobilityFlow/Pipeline"]
    }
  }
}

resource "aws_iam_role_policy" "pipeline_task" {
  name   = "${local.name}-pipeline"
  role   = aws_iam_role.pipeline_task.id
  policy = data.aws_iam_policy_document.pipeline_task.json
}
