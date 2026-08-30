output "bucket_name" {
  description = "Ephemeral session artifact bucket name."
  value       = aws_s3_bucket.artifacts.id
}

output "bucket_arn" {
  description = "Ephemeral session artifact bucket ARN."
  value       = aws_s3_bucket.artifacts.arn
}
