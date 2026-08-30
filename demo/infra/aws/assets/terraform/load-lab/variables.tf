variable "aws_region" {
  description = "AWS region for the load lab."
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

variable "runner_role_arn" {
  description = "Ephemeral runner role that receives cluster-admin access through an EKS access entry."
  type        = string
  default     = ""
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

variable "enable_runner_observability_peering" {
  description = "Create same-account VPC peering so in-cluster agents can remote_write metrics to the runner."
  type        = bool
  default     = true
}

variable "runner_project" {
  description = "Project tag used by the long-lived runner VPC."
  type        = string
  default     = "ckc-runner"
}

variable "runner_remote_write_port" {
  description = "Runner TCP port that accepts Prometheus remote_write traffic."
  type        = number
  default     = 8428
}

variable "runner_audit_port" {
  description = "Runner TCP port that receives compact audit records."
  type        = number
  default     = 5170
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
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
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

variable "kafka_mode" {
  description = "Kafka deployment mode for the load lab."
  type        = string
  default     = "kubernetes"

  validation {
    condition     = contains(["kubernetes", "msk"], var.kafka_mode)
    error_message = "kafka_mode must be either 'kubernetes' or 'msk'."
  }
}

variable "kubernetes_kafka_brokers" {
  description = "Broker count for the in-cluster Kafka deployment when kafka_mode=kubernetes."
  type        = number
  default     = 3
}

variable "msk_kafka_version" {
  description = "Apache Kafka version for the MSK cluster when kafka_mode=msk."
  type        = string
  default     = "3.7.x"
}

variable "msk_number_of_broker_nodes" {
  description = "Total number of broker nodes for the MSK cluster when kafka_mode=msk."
  type        = number
  default     = 3
}

variable "msk_broker_instance_type" {
  description = "Broker instance type for the MSK cluster when kafka_mode=msk."
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_ebs_volume_size" {
  description = "Per-broker EBS volume size in GiB for the MSK cluster when kafka_mode=msk."
  type        = number
  default     = 20
}

variable "elasticache_mode" {
  description = "Redis deployment mode for the load lab."
  type        = string
  default     = "kubernetes"

  validation {
    condition     = contains(["kubernetes", "elasticache"], var.elasticache_mode)
    error_message = "elasticache_mode must be either 'kubernetes' or 'elasticache'."
  }
}

variable "kubernetes_redis_architecture" {
  description = "Redis architecture for the in-cluster Redis deployment when elasticache_mode=kubernetes."
  type        = string
  default     = "standalone"

  validation {
    condition     = contains(["standalone", "replication"], var.kubernetes_redis_architecture)
    error_message = "kubernetes_redis_architecture must be either 'standalone' or 'replication'."
  }
}

variable "kubernetes_redis_replica_count" {
  description = "Replica count for the in-cluster Redis deployment when elasticache_mode=kubernetes and replication architecture is used."
  type        = number
  default     = 1
}

variable "elasticache_node_type" {
  description = "Node type for ElastiCache replication groups when elasticache_mode=elasticache."
  type        = string
  default     = "cache.t4g.small"
}

variable "elasticache_engine_version" {
  description = "Redis engine version for ElastiCache when elasticache_mode=elasticache."
  type        = string
  default     = "7.1"
}

variable "elasticache_num_cache_clusters" {
  description = "Node count for the ElastiCache replication group when elasticache_mode=elasticache."
  type        = number
  default     = 2
}
