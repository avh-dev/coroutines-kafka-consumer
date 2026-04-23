locals {
  name = "${var.project}-${var.environment}"

  tags = {
    Project     = var.project
    Environment = var.environment
    Owner       = var.owner
    ManagedBy   = "terraform"
    Repository  = "coroutines-kafka-consumer"
  }
}

data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_vpc" "runner" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(local.tags, {
    Name = local.name
  })
}

resource "aws_internet_gateway" "runner" {
  vpc_id = aws_vpc.runner.id

  tags = merge(local.tags, {
    Name = local.name
  })
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.runner.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = merge(local.tags, {
    Name = "${local.name}-public"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.runner.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.runner.id
  }

  tags = merge(local.tags, {
    Name = "${local.name}-public"
  })
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "runner" {
  name        = local.name
  description = "Security group for the CKC runner"
  vpc_id      = aws_vpc.runner.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, {
    Name = local.name
  })
}

resource "aws_vpc_security_group_ingress_rule" "grafana" {
  for_each = toset(var.grafana_allowed_cidrs)

  security_group_id = aws_security_group.runner.id
  cidr_ipv4         = each.value
  from_port         = 3000
  ip_protocol       = "tcp"
  to_port           = 3000
  description       = "Grafana access"
}

resource "aws_iam_role" "runner" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.runner.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "admin" {
  role       = aws_iam_role.runner.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_iam_instance_profile" "runner" {
  name = local.name
  role = aws_iam_role.runner.name
}

resource "aws_s3_bucket" "reports" {
  bucket        = "${local.name}-reports-${random_id.bucket_suffix.hex}"
  force_destroy = var.force_destroy_reports_bucket
}

resource "aws_s3_bucket_server_side_encryption_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "reports" {
  bucket = aws_s3_bucket.reports.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_instance" "runner" {
  ami                         = data.aws_ami.amazon_linux_2023.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.runner.id]
  iam_instance_profile        = aws_iam_instance_profile.runner.name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/user_data.sh")

  root_block_device {
    volume_size = var.root_volume_size
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = merge(local.tags, {
    Name = local.name
  })
}

resource "aws_eip" "runner" {
  count    = var.allocate_eip ? 1 : 0
  domain   = "vpc"
  instance = aws_instance.runner.id

  tags = merge(local.tags, {
    Name = local.name
  })
}
