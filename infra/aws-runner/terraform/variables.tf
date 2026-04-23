variable "aws_region" {
  description = "AWS region for the runner."
  type        = string
  default     = "eu-central-1"
}

variable "environment" {
  description = "Environment suffix used in names and tags."
  type        = string
  default     = "dev"
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
  description = "CIDR block for the runner public subnet."
  type        = string
  default     = "10.52.0.0/28"
}

variable "availability_zone" {
  description = "Availability zone for the runner subnet."
  type        = string
  default     = "eu-central-1a"
}

variable "instance_type" {
  description = "EC2 instance type for the runner."
  type        = string
  default     = "t3.small"
}

variable "root_volume_size" {
  description = "Root EBS volume size in GiB."
  type        = number
  default     = 50
}

variable "grafana_allowed_cidrs" {
  description = "CIDR blocks allowed to reach Grafana on port 3000. Prefer SSM port forwarding and keep this empty."
  type        = list(string)
  default     = []
}

variable "allocate_eip" {
  description = "Whether to allocate an Elastic IP for the runner. Elastic IPs can incur charges."
  type        = bool
  default     = false
}

variable "force_destroy_reports_bucket" {
  description = "Whether Terraform may delete a non-empty reports bucket."
  type        = bool
  default     = false
}
