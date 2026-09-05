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

## Run an experiment

Choose one self-contained file from `demo/infra/experiments` and select an
environment declared by that file:

```sh
demo/infra/run-experiment.sh demo/infra/experiments/smoke.yaml --environment internal-lab
demo/infra/run-experiment.sh demo/infra/experiments/smoke.yaml --environment aws
```

The shared command validates, resolves, and materializes the experiment before
dispatching it to the environment adapter. Scripts below `internal-lab/assets`
and `aws/runner-assets` are implementation backends, not operator entrypoints.

Default local observability ports are intentionally distinct:

- local-dev: Prometheus `9090`, Grafana `3000`
- internal-lab: app `30080`, Prometheus `30090`, Grafana `3000`
- AWS runner: Prometheus-compatible storage and Grafana run on the runner host

Each completed run publishes `report.md` plus images, `evidence.tar.gz`, and
`audit.tar.gz`. AWS provisioning is disposable and cleanup is verified after
artifact transport; internal-lab keeps its installed services running.

Module details are in [aws/README.md](aws/README.md), [aws/terraform/README.md](aws/terraform/README.md), [aws/assets/README.md](aws/assets/README.md), [local-dev/README.md](local-dev/README.md), and [internal-lab/README.md](internal-lab/README.md).
