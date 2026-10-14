variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Resource naming prefix"
  default     = "device-telemetry"
}

variable "environment" {
  type        = string
  description = "Environment name"
  default     = "dev"
}
