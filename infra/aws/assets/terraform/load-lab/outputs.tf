output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster API endpoint."
  value       = module.eks.cluster_endpoint
}

output "vpc_id" {
  description = "VPC identifier."
  value       = module.vpc.vpc_id
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

output "elasticache_mode" {
  description = "Redis mode used by the load lab."
  value       = var.elasticache_mode
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
