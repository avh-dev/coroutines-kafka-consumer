output "demo_ecr_repository_url" {
  description = "ECR repository URL for the demo application."
  value       = aws_ecr_repository.demo.repository_url
}

output "demo_stubs_ecr_repository_url" {
  description = "ECR repository URL for the demo stubs service."
  value       = aws_ecr_repository.demo_stubs.repository_url
}

output "load_test_ecr_repository_url" {
  description = "ECR repository URL for the load generator."
  value       = aws_ecr_repository.load_test.repository_url
}
