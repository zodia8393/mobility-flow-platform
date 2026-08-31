output "lake_bucket_name" {
  description = "Set this value as S3_BUCKET in AWS mode."
  value       = aws_s3_bucket.lake.id
}

output "ecr_repository_urls" {
  description = "Immutable service image repositories."
  value       = { for name, repository in aws_ecr_repository.service : name => repository.repository_url }
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.platform.name
}

output "pipeline_task_role_arn" {
  value = aws_iam_role.pipeline_task.arn
}

output "cloudwatch_dashboard_name" {
  value = aws_cloudwatch_dashboard.operations.dashboard_name
}

output "runtime_check_task_family" {
  description = "One-shot Fargate task used to verify ECR pull, S3 read and CloudWatch publish."
  value       = aws_ecs_task_definition.runtime_check.family
}
