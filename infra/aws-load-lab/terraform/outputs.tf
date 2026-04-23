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

output "demo_ecr_repository_url" {
  description = "ECR repository URL for the demo application."
  value       = aws_ecr_repository.demo.repository_url
}

output "load_test_ecr_repository_url" {
  description = "ECR repository URL for the load generator."
  value       = aws_ecr_repository.load_test.repository_url
}
