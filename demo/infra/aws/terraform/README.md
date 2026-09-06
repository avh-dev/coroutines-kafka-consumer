# AWS Terraform

`demo/infra/aws/terraform` contains checkout-owned Terraform stacks.

- `session-artifacts/`
  Ephemeral S3 transport for runner assets and one verified result.

- `runner/`
  Ephemeral EC2 runner with Docker, kubectl, Helm, AWS CLI, Grafana, compact
  audit ingestion, and a VictoriaMetrics remote-write receiver.

- `ecr/`
  Shared ECR repositories for the app, stubs, and load-test images.

The preferred workflow invokes all session stacks through
`scripts/run-experiment.sh`. It sets a stack-specific `TF_DATA_DIR` and explicit
local state file under `.demo-infra/experiments/aws/<session-id>`, so no state is
owned exclusively by the runner being deleted.

The manual long-lived runner workflow below is retained only for infrastructure
development, not for experiments:

```sh
cp demo/infra/aws/terraform/runner/terraform.tfvars.example demo/infra/aws/terraform/runner/terraform.tfvars
cp demo/infra/aws/terraform/ecr/terraform.tfvars.example demo/infra/aws/terraform/ecr/terraform.tfvars
./demo/infra/aws/scripts/create-runner-and-ecr.sh us-east-1 dev
```

Project experiments must use `demo/infra/run-experiment.sh`; runner-side scripts
consume generated plans and are not a separate configuration interface.

The Grafana datasource keeps the `Prometheus` name and uid so existing dashboards continue to work. Lab metrics are pushed from the EKS-side Alloy agent to the runner and remain available after `destroy-lab`.
