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
local state file under `.demo-infra/aws/sessions/<session-id>`, so no state is
owned exclusively by the runner being deleted.

The manual long-lived runner workflow below is retained for infrastructure
development, not for reproducible experiment sessions:

```sh
cp demo/infra/aws/terraform/runner/terraform.tfvars.example demo/infra/aws/terraform/runner/terraform.tfvars
cp demo/infra/aws/terraform/ecr/terraform.tfvars.example demo/infra/aws/terraform/ecr/terraform.tfvars
./demo/infra/aws/scripts/create-runner-and-ecr.sh us-east-1 dev
```

Typical perf-lab workflow after that:

```sh
./demo/infra/aws/scripts/update-aws-lab.sh us-east-1 dev
./demo/infra/aws/scripts/connect-runner.sh us-east-1
tmux new -s ckc
cd /opt/ckc-runner/assets/repo
./demo/infra/aws/runner-assets/bin/create-lab.sh us-east-1 dev default
./demo/infra/aws/runner-assets/bin/run-test.sh us-east-1 dev /path/to/materialized/resolved-test.yaml
./demo/infra/aws/runner-assets/bin/destroy-lab.sh us-east-1 dev
```

The Grafana datasource keeps the `Prometheus` name and uid so existing dashboards continue to work. Lab metrics are pushed from the EKS-side Alloy agent to the runner and remain available after `destroy-lab`.
