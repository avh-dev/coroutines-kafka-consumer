# AWS Infrastructure

![CKC AWS architecture](./architecture.svg)

`infra/aws` is now split by responsibility instead of by historical module name.

## Layout

- `terraform/`
  Long-lived Terraform stacks managed from the local machine.
  This currently holds `runner/` and `ecr/`.

- `assets/`
  Files uploaded to the runner by `update-runner`.
  This includes runner-side Terraform, Helm charts, test definitions, and remote runner entrypoints.

- `scripts/`
  Local operator commands split by OS:
  `scripts/linux/`
  `scripts/windows/`

## Model

- `terraform/runner`
  Creates the long-lived private EC2 runner.

- `terraform/ecr`
  Creates the long-lived ECR repositories.

- `assets/terraform`
  Defines the disposable AWS test lab synced to the runner.

- `assets/runner`
  Contains remote scripts that execute on the runner and orchestrate lab lifecycle and test runs.

- `assets/helm`
  Helm charts and deployment profiles for the app and stub workloads.

- `assets/test-definitions`
  Test-run definitions that select deployment profiles and load configuration.

## Access Model

- The runner has no public IP.
- Shell access uses AWS Systems Manager Session Manager.
- Grafana and Prometheus are exposed locally through SSM port forwarding.
- Outbound internet access from the runner goes through a NAT gateway.

## Typical Flow

1. Create the long-lived base with `create-runner-and-ecr`.
2. Build and push images from your workstation.
3. Create or reuse a lab from your workstation with `create-lab`.
4. Run one or more tests with `run-test`.
5. Destroy the lab with `destroy-lab` when it is no longer needed.

See [terraform/README.md](/C:/Users/Alexey/code/coroutines-kafka-consumer/infra/aws/terraform/README.md) and [assets/README.md](/C:/Users/Alexey/code/coroutines-kafka-consumer/infra/aws/assets/README.md).
