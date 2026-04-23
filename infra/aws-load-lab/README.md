# AWS Load Lab

This directory contains the first cloud-ready scaffold for running the demo application, stubs, and load tests on AWS.

## Target Shape

The first iteration intentionally uses one EKS cluster and separates workloads by namespace:

- `ckc-app`: demo application, Kafka, Redis, stubs, Prometheus, Grafana
- `ckc-loadtest`: indexed jobs or ad-hoc jobs that generate traffic

Why one cluster first:

- lower fixed cost than two EKS control planes
- simpler networking and observability
- enough to validate throughput, scaling, and most app-level resiliency scenarios

When to split into two clusters later:

- you want to measure noisy-neighbor effects from the generator independently
- you need blast-radius isolation between system under test and test harness
- you want to simulate cross-cluster or cross-VPC failure modes

## Recommended Rollout Order

1. Create AWS account and enable MFA on the root user.
2. Create an admin IAM user or, preferably, an IAM Identity Center setup for daily work.
3. Install local tools: `aws`, `kubectl`, `helm`, `terraform`, `docker`.
4. Create the AWS base stack from `terraform/`.
5. Build and push container images to ECR.
6. Connect to EKS and deploy app workloads.
7. Run load-test jobs.
8. Collect metrics and logs.
9. Destroy workloads and then destroy Terraform resources.

## What Terraform Creates

The Terraform stack in `terraform/` provisions:

- VPC with public and private subnets
- one EKS cluster with one managed node group
- two ECR repositories for the demo app and load-test generator
- security groups and IAM roles handled by the upstream modules

It does not yet provision Kafka and Redis as AWS managed services.
That is deliberate:

- your app currently expects plain Kafka bootstrap servers and a plain Redis endpoint
- MSK Serverless would require IAM-based Kafka auth changes in the clients
- in-cluster Kafka and Redis let you move to AWS quickly without changing app code first

## First Practical Topology

- EKS namespace `ckc-app`
- Kafka in-cluster via Helm chart
- Redis in-cluster via Helm chart
- `demo-stubs` deployment
- `demo` deployment
- Prometheus and Grafana in-cluster
- EKS namespace `ckc-loadtest`
- load test as `Job` or indexed `Job`

## Files

- `terraform/`: AWS infrastructure
- `scripts/build-and-push.sh`: build images and push to ECR
- `scripts/deploy-lab.sh`: create namespaces and print the next deployment steps
- `scripts/destroy-lab.sh`: remove namespaces and destroy Terraform

## Step By Step

### 1. Prepare AWS credentials

```sh
aws configure
aws sts get-caller-identity
```

### 2. Create the base infrastructure

```sh
cd infra/aws-load-lab/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

### 3. Build and push images

From the repository root:

```sh
./infra/aws-load-lab/scripts/build-and-push.sh eu-central-1 dev
```

### 4. Connect kubectl to the cluster

```sh
aws eks update-kubeconfig --region eu-central-1 --name ckc-load-lab-dev
kubectl get nodes
```

### 5. Install Kafka and Redis into the app namespace

Start with in-cluster services to avoid application code changes.

Recommended chart choices:

- Kafka: Bitnami Kafka in KRaft mode
- Redis: Bitnami Redis

Use persistence classes appropriate for the node group disks and keep replica counts minimal for the first iteration.

### 6. Deploy the app stack

Set these runtime values for the demo application:

- `SPRING_DATA_REDIS_HOST`
- `SPRING_DATA_REDIS_PORT`
- `DEMO_KAFKA_ENABLED=true`
- `DEMO_KAFKA_BOOTSTRAP_SERVERS`
- `MODEL_BASE_URL`

Set these runtime values for the load-test job:

- `BOOTSTRAP_SERVERS`
- `ORDER_LIFECYCLE_TOPIC`
- `CAULDRON_TELEMETRY_TOPIC`
- `BASE_RATE`
- `LOAD_PROFILE`
- `TOTAL_SHARDS`

### 7. Run tests

Start with a single shard and a conservative profile, then move to indexed jobs with multiple shards.

Suggested first profile:

```text
0 -> (120s, warmup) -> 50 -> (600s, steady) -> 50 -> (60s, cool-down) -> 0
```

### 8. Tear down

```sh
./infra/aws-load-lab/scripts/destroy-lab.sh eu-central-1 dev
```

## Git Bash

These scripts are intended to run from `git bash` on Windows.

Expected prerequisites:

- `aws`, `kubectl`, `terraform`, and `docker` are installed on Windows
- each binary is available in the `PATH` seen by `git bash`
- Docker Desktop is running before image build and push steps

Quick check:

```sh
which aws
which kubectl
which terraform
which docker
```

One practical note: `aws eks update-kubeconfig` writes Windows user kubeconfig data and works fine from `git bash` in the usual setup.

## Cost Guardrails

- keep exactly one EKS node group at first
- use one small node size for control experiments, then scale only when needed
- use one cluster, two namespaces
- destroy the entire stack after each test window
- tag all resources with environment and owner

## Next Iterations

After the base path works, the logical next steps are:

1. add Kubernetes manifests or Helm values for Kafka, Redis, demo, stubs, Prometheus, and Grafana
2. add indexed jobs for the load generator
3. publish dashboards and alerting rules
4. decide whether Kafka should stay in-cluster or move to MSK
5. decide whether Redis should stay in-cluster or move to ElastiCache
