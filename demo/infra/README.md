# Infrastructure

`demo/infra/` is the entrypoint for local demo services, the internal k3s lab, and AWS-based load testing.

## Layout

- `local-dev/`: local Kafka, Redis, Prometheus, and Grafana for fast demo development with Docker Compose
- `internal-lab/`: lightweight k3s environment for a dedicated Linux laptop, with host-managed Kafka, Redis, Grafana, and stubs
- `shared/`: test orchestration, audit tooling, Grafana provisioning, and dashboards shared by lab environments
- `aws/terraform/`: long-lived AWS Terraform stacks for `runner` and `ecr`
- `aws/assets/`: AWS-only assets uploaded to the runner, including disposable `load-lab` Terraform
- `aws/runner-assets/`: scripts executed inside the AWS runner
- `aws/scripts/`: Git Bash-compatible local operator scripts

## Shell Notes

On Windows, run local infrastructure shell scripts from Git Bash.
Do not use PowerShell wrappers for demo infrastructure; the local scripts assume Git Bash path behavior when used on Windows.

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
cp demo/infra/aws/terraform/runner/terraform.tfvars.example demo/infra/aws/terraform/runner/terraform.tfvars
cp demo/infra/aws/terraform/ecr/terraform.tfvars.example demo/infra/aws/terraform/ecr/terraform.tfvars
./demo/infra/aws/scripts/create-runner-and-ecr.sh us-east-1 dev
```

What you get after `apply`:

- a private EC2 runner in AWS
- Prometheus-compatible metrics storage running on the runner
- Grafana running on the runner
- the shared `CKC Overview` dashboard already provisioned
- no public inbound access to the instance

Connect to the runner:

```sh
./demo/infra/aws/scripts/connect-runner.sh us-east-1
```

Start long-running AWS lab work from the runner, preferably inside `tmux`:

```sh
tmux new -s ckc
cd /opt/ckc-runner/assets/repo
./demo/infra/aws/runner-assets/bin/create-lab.sh us-east-1 dev default
./demo/infra/aws/runner-assets/bin/run-test.sh us-east-1 dev /path/to/materialized/resolved-test.yaml
```

Update images and runner assets from your local machine when the code changes:

```sh
./demo/infra/aws/scripts/update-aws-lab.sh us-east-1 dev
```

Default local observability ports are intentionally distinct:

- local-dev: Prometheus `9090`, Grafana `3000`
- internal-lab: app `30080`, Prometheus `30090`, Grafana `3000`
- AWS runner: Prometheus-compatible storage and Grafana run on the runner host

## Typical Workflow

1. Create the runner and ECR repositories from your local machine.
2. Update AWS lab images and runner assets from your local machine when needed.
3. Connect to the runner from your local machine.
4. Run lab creation, tests, chaos scenarios, and lab cleanup on the runner inside `tmux`.
5. Destroy the long-lived runner and ECR stacks manually with Terraform only when they are no longer needed.

Module details are in [aws/README.md](aws/README.md), [aws/terraform/README.md](aws/terraform/README.md), [aws/assets/README.md](aws/assets/README.md), [local-dev/README.md](local-dev/README.md), and [internal-lab/README.md](internal-lab/README.md).
