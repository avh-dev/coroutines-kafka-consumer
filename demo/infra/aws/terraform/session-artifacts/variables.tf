variable "aws_region" {
  description = "AWS region for the ephemeral artifact bucket."
  type        = string
  default     = "us-east-1"
}

variable "session_id" {
  description = "Unique lowercase identifier for one ephemeral AWS session."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,34}$", var.session_id))
    error_message = "session_id must be 3-35 lowercase letters, digits, or hyphens."
  }
}

variable "experiment_id" {
  description = "Experiment identifier used for cost attribution."
  type        = string
}

variable "owner" {
  description = "Owner tag for session resources."
  type        = string
  default     = "local-user"
}

variable "expires_at" {
  description = "UTC cleanup deadline recorded as a tag."
  type        = string
}

variable "project" {
  description = "Project tag for session resources."
  type        = string
  default     = "ckc-aws-experiment"
}
