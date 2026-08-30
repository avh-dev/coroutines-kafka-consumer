variable "aws_region" {
  description = "AWS region for the runner."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment suffix used in names and tags."
  type        = string
  default     = "dev"
}

variable "session_id" {
  description = "Unique AWS experiment session identifier."
  type        = string
  default     = "dev"
}

variable "experiment_id" {
  description = "Experiment identifier used for cost attribution."
  type        = string
  default     = "manual"
}

variable "expires_at" {
  description = "UTC cleanup deadline recorded as a tag."
  type        = string
  default     = "manual"
}

variable "artifact_bucket_arn" {
  description = "ARN of the ephemeral bucket used to exchange runner assets and results."
  type        = string
  default     = ""
}

variable "owner" {
  description = "Owner tag for runner resources."
  type        = string
  default     = "local-user"
}

variable "project" {
  description = "Project name used in tags and names."
  type        = string
  default     = "ckc-runner"
}

variable "vpc_cidr" {
  description = "CIDR block for the runner VPC."
  type        = string
  default     = "10.52.0.0/24"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the runner public subnet used by the NAT gateway."
  type        = string
  default     = "10.52.0.0/28"
}

variable "private_subnet_cidr" {
  description = "CIDR block for the runner private subnet."
  type        = string
  default     = "10.52.0.16/28"
}

variable "availability_zone" {
  description = "Availability zone for the runner subnets."
  type        = string
  default     = "us-east-1a"
}

variable "instance_type" {
  description = "EC2 instance type for the runner."
  type        = string
  default     = "t3.small"
}

variable "root_volume_size" {
  description = "Root EBS volume size in GiB."
  type        = number
  default     = 20
}

variable "prometheus_scrape_interval" {
  description = "Prometheus scrape interval for the runner."
  type        = string
  default     = "30s"
}

variable "prometheus_evaluation_interval" {
  description = "Prometheus rule evaluation interval for the runner."
  type        = string
  default     = "30s"
}

variable "prometheus_retention_size" {
  description = "Prometheus TSDB retention size for the runner."
  type        = string
  default     = "2GB"
}
