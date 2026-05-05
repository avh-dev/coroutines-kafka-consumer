output "instance_id" {
  description = "Runner EC2 instance id."
  value       = aws_instance.runner.id
}

output "private_ip" {
  description = "Runner private IP address."
  value       = aws_instance.runner.private_ip
}

output "ssm_start_session_command" {
  description = "Command to open an SSM session."
  value       = "aws ssm start-session --target ${aws_instance.runner.id} --region ${var.aws_region}"
}

output "grafana_port_forward_command" {
  description = "Command to forward local port 3002 to Grafana over SSM."
  value       = "aws ssm start-session --target ${aws_instance.runner.id} --document-name AWS-StartPortForwardingSession --parameters portNumber=3000,localPortNumber=3002 --region ${var.aws_region}"
}

output "prometheus_port_forward_command" {
  description = "Command to forward local port 9093 to Prometheus over SSM."
  value       = "aws ssm start-session --target ${aws_instance.runner.id} --document-name AWS-StartPortForwardingSession --parameters portNumber=9090,localPortNumber=9093 --region ${var.aws_region}"
}
