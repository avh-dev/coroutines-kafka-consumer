output "instance_id" {
  description = "Runner EC2 instance id."
  value       = aws_instance.runner.id
}

output "public_ip" {
  description = "Runner public IP address."
  value       = var.allocate_eip ? aws_eip.runner[0].public_ip : aws_instance.runner.public_ip
}

output "reports_bucket" {
  description = "S3 bucket for reports."
  value       = aws_s3_bucket.reports.bucket
}

output "ssm_start_session_command" {
  description = "Command to open an SSM session."
  value       = "aws ssm start-session --target ${aws_instance.runner.id} --region ${var.aws_region}"
}

output "grafana_url" {
  description = "Grafana URL if port 3000 is allowed by security group."
  value       = "http://${var.allocate_eip ? aws_eip.runner[0].public_ip : aws_instance.runner.public_ip}:3000"
}
