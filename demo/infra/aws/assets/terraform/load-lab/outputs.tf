output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster API endpoint."
  value       = module.eks.cluster_endpoint
}

output "kubernetes_version" {
  description = "Resolved EKS Kubernetes version."
  value       = var.kubernetes_version
}

output "node_instance_types" {
  description = "Instance types accepted by the EKS worker node group."
  value       = var.node_instance_types
}

output "node_desired_size" {
  description = "Requested EKS worker node count."
  value       = var.node_desired_size
}

output "node_disk_size" {
  description = "Per-worker EBS volume size in GiB."
  value       = var.node_disk_size
}

output "dedicated_node_groups" {
  description = "Whether support and application workloads use separate managed node groups."
  value       = var.dedicated_node_groups
}

output "node_groups" {
  description = "Resolved EKS managed node-group capacity used in experiment evidence."
  value = merge(
    var.dedicated_node_groups ? {
      support = {
        instance_types = var.support_node_instance_types
        desired_size   = var.support_node_desired_size
        min_size       = var.support_node_min_size
        max_size       = var.support_node_max_size
        disk_size_gib  = var.support_node_disk_size
        autoscaling    = false
      }
      application = {
        instance_types = var.application_node_instance_types
        desired_size   = var.application_node_desired_size
        min_size       = var.application_node_min_size
        max_size       = var.application_node_max_size
        disk_size_gib  = var.application_node_disk_size
        autoscaling    = true
      }
    } : {},
    var.dedicated_node_groups ? {} : {
      shared = {
        instance_types = var.node_instance_types
        desired_size   = var.node_desired_size
        min_size       = var.node_min_size
        max_size       = var.node_max_size
        disk_size_gib  = var.node_disk_size
        autoscaling    = false
      }
    }
  )
}

output "cluster_autoscaler_image" {
  description = "Cluster Autoscaler image for the provisioned Kubernetes version."
  value       = var.dedicated_node_groups ? var.cluster_autoscaler_image : ""
}

output "vpc_id" {
  description = "VPC identifier."
  value       = module.vpc.vpc_id
}

output "runner_observability_peering_enabled" {
  description = "Whether load-lab created VPC peering to the runner for remote_write."
  value       = var.enable_runner_observability_peering
}

output "private_subnet_ids" {
  description = "Private subnets used by EKS worker nodes."
  value       = module.vpc.private_subnets
}

output "kafka_mode" {
  description = "Kafka mode used by the load lab."
  value       = var.kafka_mode
}

output "kubernetes_kafka_brokers" {
  description = "Broker count for the in-cluster Kafka deployment."
  value       = var.kubernetes_kafka_brokers
}

output "msk_bootstrap_brokers" {
  description = "Plaintext bootstrap brokers for the MSK cluster when kafka_mode=msk."
  value       = try(aws_msk_cluster.load_lab[0].bootstrap_brokers, "")
}

output "msk_number_of_broker_nodes" {
  description = "Broker count for the MSK cluster."
  value       = var.msk_number_of_broker_nodes
}

output "msk_kafka_version" {
  description = "MSK Kafka version."
  value       = var.msk_kafka_version
}

output "msk_broker_instance_type" {
  description = "MSK broker instance type."
  value       = var.msk_broker_instance_type
}

output "msk_ebs_volume_size" {
  description = "Per-MSK-broker EBS volume size in GiB."
  value       = var.msk_ebs_volume_size
}

output "elasticache_mode" {
  description = "Redis mode used by the load lab."
  value       = var.elasticache_mode
}

output "elasticache_node_type" {
  description = "ElastiCache node type used by the load lab."
  value       = var.elasticache_node_type
}

output "elasticache_engine_version" {
  description = "Redis engine version used by ElastiCache."
  value       = var.elasticache_engine_version
}

output "kubernetes_redis_architecture" {
  description = "Redis architecture for the in-cluster Redis deployment."
  value       = var.kubernetes_redis_architecture
}

output "kubernetes_redis_replica_count" {
  description = "Replica count for the in-cluster Redis deployment."
  value       = var.kubernetes_redis_replica_count
}

output "elasticache_primary_endpoint" {
  description = "Primary endpoint for the ElastiCache replication group when elasticache_mode=elasticache."
  value       = try(aws_elasticache_replication_group.load_lab[0].primary_endpoint_address, "")
}

output "elasticache_member_clusters" {
  description = "Cache cluster identifiers backing the ElastiCache replication group."
  value       = try(aws_elasticache_replication_group.load_lab[0].member_clusters, [])
}
