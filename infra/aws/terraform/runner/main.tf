locals {
  name               = "${var.project}-${var.environment}"
  observability_root = "${path.module}/../../../shared/grafana"

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

resource "aws_instance" "runner" {
  ami                         = data.aws_ami.amazon_linux_2023.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.runner.id]
  iam_instance_profile        = aws_iam_instance_profile.runner.name
  associate_public_ip_address = true
  user_data_replace_on_change = true
  user_data_base64 = base64gzip(templatefile("${path.module}/user_data.sh.tftpl", {
    docker_compose = templatefile("${path.module}/templates/docker-compose.yml.tftpl", {
      prometheus_retention_size = var.prometheus_retention_size
    })
    default_prometheus_config = templatefile("${path.module}/templates/prometheus.yml.tftpl", {
      prometheus_scrape_interval     = var.prometheus_scrape_interval
      prometheus_evaluation_interval = var.prometheus_evaluation_interval
    })
    grafana_dashboard_provider_config    = file("${local.observability_root}/provisioning/dashboards/ckc.yml")
    grafana_prometheus_datasource_config = file("${local.observability_root}/provisioning/datasources/prometheus.yml")
    grafana_ckc_overview_dashboard       = file("${local.observability_root}/dashboards/ckc-overview.json")
    configure_observability_script_content = templatefile("${path.module}/templates/configure-observability.sh.tftpl", {
      prometheus_scrape_interval     = var.prometheus_scrape_interval
      prometheus_evaluation_interval = var.prometheus_evaluation_interval
    })
  }))

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

  depends_on = [aws_route_table_association.public]
}
