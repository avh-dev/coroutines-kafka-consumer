variable "aws_region" {
  description = "AWS region for the load lab."
  type        = string
  default     = "eu-central-1"
}

variable "environment" {
  description = "Environment suffix used in names and tags."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Owner tag for the lab resources."
  type        = string
  default     = "local-user"
}

variable "project" {
  description = "Project name used in tags and names."
  type        = string
  default     = "ckc-load-lab"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.33"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones used by the VPC and EKS."
  type        = list(string)
  default     = ["eu-central-1a", "eu-central-1b", "eu-central-1c"]
}

variable "private_subnets" {
  description = "Private subnets for worker nodes."
  type        = list(string)
  default     = ["10.42.0.0/20", "10.42.16.0/20", "10.42.32.0/20"]
}

variable "public_subnets" {
  description = "Public subnets for ingress or future public services."
  type        = list(string)
  default     = ["10.42.128.0/20", "10.42.144.0/20", "10.42.160.0/20"]
}

variable "node_instance_types" {
  description = "Managed node group EC2 instance types."
  type        = list(string)
  default     = ["m7i.large"]
}

variable "node_desired_size" {
  description = "Desired node count for the initial node group."
  type        = number
  default     = 2
}

variable "node_min_size" {
  description = "Minimum node count for the initial node group."
  type        = number
  default     = 1
}

variable "node_max_size" {
  description = "Maximum node count for the initial node group."
  type        = number
  default     = 4
}

variable "node_disk_size" {
  description = "Disk size in GiB for EKS worker nodes."
  type        = number
  default     = 100
}
