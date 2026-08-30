data "aws_caller_identity" "current" {}

locals {
  bucket_name = "ckc-experiment-${data.aws_caller_identity.current.account_id}-${var.session_id}"

  tags = {
    Project      = var.project
    ExperimentId = var.experiment_id
    SessionId    = var.session_id
    Owner        = var.owner
    ExpiresAt    = var.expires_at
    ManagedBy    = "terraform"
    Repository   = "coroutines-kafka-consumer"
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = local.bucket_name
  force_destroy = true

  tags = merge(local.tags, {
    Name = local.bucket_name
  })
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }

    expiration {
      days = 7
    }
  }
}
