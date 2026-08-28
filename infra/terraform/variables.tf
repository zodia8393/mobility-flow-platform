variable "project_name" {
  description = "Resource name prefix."
  type        = string
  default     = "mobility-flow"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "project_name must be a lowercase AWS-safe name."
  }
}

variable "environment" {
  description = "Deployment environment label."
  type        = string
  default     = "portfolio"
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-northeast-2"
}

variable "bucket_name" {
  description = "Optional globally unique bucket name. Empty uses project-account-region."
  type        = string
  default     = ""
}

variable "alert_email" {
  description = "Optional email endpoint for freshness and quality alarms."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 14

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90], var.log_retention_days)
    error_message = "Use a supported CloudWatch retention period."
  }
}
