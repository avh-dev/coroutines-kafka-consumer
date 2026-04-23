# AWS Runner

This directory defines a small long-lived management host for running cloud load-test scenarios.

The runner is intentionally separate from `infra/aws-load-lab`:

- `aws-runner`: persistent EC2 host, S3 reports bucket, Prometheus, Grafana, orchestration scripts
- `aws-load-lab`: temporary test infrastructure, such as EKS, ECR, Kafka, Redis, app workloads, and load-test jobs

## Target Model

```text
EC2 runner
  - aws cli, terraform, kubectl, helm, docker
  - docker compose:
      prometheus
      grafana
  - scripts:
      run load lab
      collect reports
      destroy temporary resources
  - persistent EBS data:
      /opt/ckc-runner/prometheus
      /opt/ckc-runner/grafana
      /opt/ckc-runner/reports
```

The runner can be stopped when not in use. Stopped EC2 compute is not charged, but attached EBS volumes and optional Elastic IPs can still incur charges.

## What Terraform Creates

- small management VPC
- public subnet and internet gateway
- EC2 runner instance
- IAM role for SSM, ECR, EKS, S3, CloudWatch, and temporary lab administration
- S3 bucket for reports
- optional Grafana ingress on port `3000`
- optional Elastic IP

SSH is not opened by default. Use AWS Systems Manager Session Manager.

## Create Runner

From this directory:

```sh
cd infra/aws-runner/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

If you want browser access to Grafana without SSM port forwarding, set `grafana_allowed_cidrs` in `terraform.tfvars` to your public IP CIDR:

```hcl
grafana_allowed_cidrs = ["203.0.113.10/32"]
```

## Connect

Use Session Manager:

```sh
aws ssm start-session --target <instance-id> --region eu-central-1
```

If the SSM session is not available immediately, wait a few minutes. The instance bootstraps Docker and CLI tools on first boot.

## Start Prometheus and Grafana

On the runner:

```sh
cd /opt/ckc-runner/repo
./infra/aws-runner/scripts/start-observability.sh app-metrics.example.com:8080 /actuator/prometheus
```

Grafana runs on port `3000`.

Default credentials:

- user: `admin`
- password: `admin`

Change the password after first login if the endpoint is public.

## Run Temporary Load Lab

On the runner, after cloning or updating this repository:

```sh
cd /opt/ckc-runner/repo
./infra/aws-runner/scripts/run-load-lab.sh eu-central-1 dev
```

This script:

1. applies `infra/aws-load-lab/terraform`
2. builds and pushes demo/load-test images
3. prepares EKS namespaces

It does not destroy the lab automatically. Destroy explicitly after you collect data:

```sh
./infra/aws-load-lab/scripts/destroy-lab.sh eu-central-1 dev
```

## Stop Observability

```sh
./infra/aws-runner/scripts/stop-observability.sh
```

## Stop Runner

When you are done looking at dashboards:

```sh
aws ec2 stop-instances --instance-ids <instance-id> --region eu-central-1
```

Start it later:

```sh
aws ec2 start-instances --instance-ids <instance-id> --region eu-central-1
```

## Notes

- Keep Prometheus retention short for this lab, for example `3d`.
- Prefer SSM port forwarding over public Grafana access.
- Keep `allocate_eip = false` unless you need a stable public IP.
- The runner IAM role is broad enough for lab administration; tighten it after the workflow stabilizes.
