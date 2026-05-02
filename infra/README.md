# Infrastructure

`infra/` is the entrypoint for local demo services, local Kubernetes test runs, and AWS-based load testing.

## Layout

- `local-dev/`: local Kafka, Redis, Prometheus, and Grafana for fast demo development with Docker Compose
- `local-k8s/`: minikube-based test environment that reuses the shared Helm charts and test orchestration
- `shared/`: Helm charts, test definitions, test orchestration, Grafana provisioning, and dashboards shared by local and AWS environments
- `aws/terraform/`: long-lived AWS Terraform stacks for `runner` and `ecr`
- `aws/assets/`: AWS-only assets uploaded to the runner, including disposable `load-lab` Terraform
- `aws/runner-internal/`: scripts executed inside the AWS runner
- `aws/scripts/`: local operator scripts split into `linux` and `windows`

## AWS Model

The AWS flow is split into two parts:

1. `terraform/runner`
   A long-lived private EC2 host in `us-east-1` with Docker, Terraform, kubectl, Helm, AWS CLI, Grafana, and Prometheus-compatible metrics storage.
   Access is through AWS Systems Manager Session Manager only.
   The default root volume is `20 GiB`, and metrics retention storage is kept on the runner so lab history survives lab destroy.

2. `assets/terraform/load-lab`
   A temporary EKS-based test environment used to deploy the app under test, supporting services, and load generators.
   It is intended to be created for a test window, used to collect pod-aware metrics in Grafana on the runner through in-cluster Alloy remote_write, and then destroyed to avoid ongoing cost.

## Quick Start

From the repository root, prepare the runner:

```sh
cp infra/aws/terraform/runner/terraform.tfvars.example infra/aws/terraform/runner/terraform.tfvars
cp infra/aws/terraform/ecr/terraform.tfvars.example infra/aws/terraform/ecr/terraform.tfvars
./infra/aws/scripts/linux/create-runner-and-ecr.sh us-east-1 dev
```

Or on Windows PowerShell:

```powershell
Copy-Item infra\aws\terraform\runner\terraform.tfvars.example infra\aws\terraform\runner\terraform.tfvars
Copy-Item infra\aws\terraform\ecr\terraform.tfvars.example infra\aws\terraform\ecr\terraform.tfvars
./infra/aws/scripts/windows/create-runner-and-ecr.ps1 -Region us-east-1 -Environment dev
```

What you get after `apply`:

- a private EC2 runner in AWS
- Prometheus-compatible metrics storage running on the runner
- Grafana running on the runner
- the shared `CKC Overview` dashboard already provisioned
- no public inbound access to the instance

Open Grafana from your local machine through SSM port forwarding:

```sh
./infra/aws/scripts/linux/start-grafana-tunnel.sh us-east-1
```

Or on Windows PowerShell:

```powershell
./infra/aws/scripts/windows/start-grafana-tunnel.ps1 -Region us-east-1
```

Then browse to `http://localhost:3000` with `admin` / `admin`.

Open a shell on the runner:

```sh
./infra/aws/scripts/linux/connect-runner.sh us-east-1
```

## Typical Workflow

1. Create the runner and ECR repositories from your local machine.
2. Open Grafana and Prometheus tunnels from your local machine.
3. Start a test from your workstation with `start-test`.
4. Inspect Grafana and Prometheus through local SSM tunnels.
5. Destroy the runner only when you no longer need the long-lived management host.

## Cleanup

To destroy both the temporary load lab and the runner:

```sh
./infra/aws/scripts/linux/destroy-all.sh us-east-1 dev
```

Or on Windows PowerShell:

```powershell
./infra/aws/scripts/windows/destroy-all.ps1 -Region us-east-1 -Environment dev
```

Module details are in [aws/README.md](aws/README.md), [aws/terraform/README.md](aws/terraform/README.md), [aws/assets/README.md](aws/assets/README.md), [local-dev/README.md](local-dev/README.md), and [local-k8s/README.md](local-k8s/README.md).
