locals {
  name                            = "${var.project}-${var.environment}"
  msk_internal_replication_factor = min(var.msk_number_of_broker_nodes, 3)
  msk_min_insync_replicas         = max(local.msk_internal_replication_factor - 1, 1)

  tags = {
    Project     = var.project
    Environment = var.environment
    Owner       = var.owner
    ManagedBy   = "terraform"
    Repository  = "coroutines-kafka-consumer"
  }
}

data "aws_vpc" "runner" {
  count = var.enable_runner_observability_peering ? 1 : 0

  filter {
    name   = "tag:Project"
    values = [var.runner_project]
  }

  filter {
    name   = "tag:Environment"
    values = [var.environment]
  }
}

data "aws_route_tables" "runner" {
  count  = var.enable_runner_observability_peering ? 1 : 0
  vpc_id = data.aws_vpc.runner[0].id
}

data "aws_security_group" "runner" {
  count = var.enable_runner_observability_peering ? 1 : 0

  filter {
    name   = "tag:Name"
    values = ["${var.runner_project}-${var.environment}"]
  }

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.runner[0].id]
  }
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = local.name
  cidr = var.vpc_cidr

  azs             = var.availability_zones
  private_subnets = var.private_subnets
  public_subnets  = var.public_subnets

  enable_nat_gateway     = true
  single_nat_gateway     = true
  one_nat_gateway_per_az = false
  enable_dns_hostnames   = true
  enable_dns_support     = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }

  tags = local.tags
}

resource "aws_vpc_peering_connection" "runner" {
  count       = var.enable_runner_observability_peering ? 1 : 0
  vpc_id      = module.vpc.vpc_id
  peer_vpc_id = data.aws_vpc.runner[0].id
  auto_accept = true

  tags = merge(local.tags, {
    Name = "${local.name}-to-${var.runner_project}-${var.environment}"
  })
}

resource "aws_route" "load_lab_to_runner" {
  count                     = var.enable_runner_observability_peering ? length(module.vpc.private_route_table_ids) : 0
  route_table_id            = module.vpc.private_route_table_ids[count.index]
  destination_cidr_block    = data.aws_vpc.runner[0].cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.runner[0].id
}

resource "aws_route" "runner_to_load_lab" {
  count                     = var.enable_runner_observability_peering ? length(data.aws_route_tables.runner[0].ids) : 0
  route_table_id            = data.aws_route_tables.runner[0].ids[count.index]
  destination_cidr_block    = var.vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.runner[0].id
}

resource "aws_security_group_rule" "runner_remote_write_from_load_lab" {
  count             = var.enable_runner_observability_peering ? 1 : 0
  type              = "ingress"
  security_group_id = data.aws_security_group.runner[0].id
  description       = "Prometheus remote_write from CKC load-lab"
  from_port         = var.runner_remote_write_port
  to_port           = var.runner_remote_write_port
  protocol          = "tcp"
  cidr_blocks       = [var.vpc_cidr]
}

resource "aws_security_group" "msk" {
  count = var.kafka_mode == "msk" ? 1 : 0

  name        = "${local.name}-msk"
  description = "Security group for the CKC load-lab MSK cluster"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Kafka plaintext from inside the load-lab VPC"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, {
    Name = "${local.name}-msk"
  })
}

resource "aws_security_group" "elasticache" {
  count = var.elasticache_mode == "elasticache" ? 1 : 0

  name        = "${local.name}-elasticache"
  description = "Security group for the CKC load-lab ElastiCache cluster"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Redis from inside the load-lab VPC"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, {
    Name = "${local.name}-elasticache"
  })
}

resource "aws_msk_configuration" "load_lab" {
  count = var.kafka_mode == "msk" ? 1 : 0

  kafka_versions = [var.msk_kafka_version]
  name           = "${local.name}-msk"
  description    = "CKC load-lab MSK configuration"

  server_properties = <<-EOT
    auto.create.topics.enable=true
    default.replication.factor=${local.msk_internal_replication_factor}
    min.insync.replicas=${local.msk_min_insync_replicas}
    offsets.topic.replication.factor=${local.msk_internal_replication_factor}
    transaction.state.log.replication.factor=${local.msk_internal_replication_factor}
    transaction.state.log.min.isr=${local.msk_min_insync_replicas}
  EOT
}

resource "aws_msk_cluster" "load_lab" {
  count = var.kafka_mode == "msk" ? 1 : 0

  cluster_name           = "${local.name}-msk"
  kafka_version          = var.msk_kafka_version
  number_of_broker_nodes = var.msk_number_of_broker_nodes
  enhanced_monitoring    = "DEFAULT"

  broker_node_group_info {
    instance_type   = var.msk_broker_instance_type
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk[0].id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_ebs_volume_size
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.load_lab[0].arn
    revision = aws_msk_configuration.load_lab[0].latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = true
    }
  }

  client_authentication {
    unauthenticated = true
  }

  tags = merge(local.tags, {
    Name = "${local.name}-msk"
  })
}

resource "aws_elasticache_subnet_group" "load_lab" {
  count = var.elasticache_mode == "elasticache" ? 1 : 0

  name       = "${local.name}-elasticache"
  subnet_ids = module.vpc.private_subnets

  tags = merge(local.tags, {
    Name = "${local.name}-elasticache"
  })
}

resource "aws_elasticache_replication_group" "load_lab" {
  count = var.elasticache_mode == "elasticache" ? 1 : 0

  replication_group_id       = replace("${local.name}-redis", "_", "-")
  description                = "CKC load-lab ElastiCache replication group"
  engine                     = "redis"
  engine_version             = var.elasticache_engine_version
  node_type                  = var.elasticache_node_type
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.load_lab[0].name
  security_group_ids         = [aws_security_group.elasticache[0].id]
  automatic_failover_enabled = var.elasticache_num_cache_clusters > 1
  multi_az_enabled           = var.elasticache_num_cache_clusters > 1
  num_cache_clusters         = var.elasticache_num_cache_clusters
  at_rest_encryption_enabled = false
  transit_encryption_enabled = false
  apply_immediately          = true
  snapshot_retention_limit   = 0

  tags = merge(local.tags, {
    Name = "${local.name}-elasticache"
  })
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = local.name
  kubernetes_version = var.kubernetes_version

  endpoint_public_access                   = true
  enable_cluster_creator_admin_permissions = true
  create_kms_key                           = false
  encryption_config                        = null
  create_cloudwatch_log_group              = false

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  addons = {
    vpc-cni = {
      before_compute = true
    }
    coredns                = {}
    kube-proxy             = {}
    eks-pod-identity-agent = {}
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      desired_size   = var.node_desired_size
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      disk_size      = var.node_disk_size

      labels = {
        workload = "general"
      }
    }
  }

  tags = local.tags
}
