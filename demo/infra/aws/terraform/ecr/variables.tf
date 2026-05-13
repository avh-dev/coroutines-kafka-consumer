variable "aws_region" {
  description = "AWS region for the ECR repositories."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment suffix used in names and tags."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Owner tag for the ECR resources."
  type        = string
  default     = "local-user"
}

variable "project" {
  description = "Project name used in tags and names."
  type        = string
  default     = "ckc"
}
