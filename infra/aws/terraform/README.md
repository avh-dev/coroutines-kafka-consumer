# AWS Terraform

`infra/aws/terraform` contains long-lived Terraform stacks that are applied from the local machine.

- `runner/`
  Private EC2 runner with Docker, Terraform, kubectl, Helm, AWS CLI, Grafana, and a Prometheus-compatible VictoriaMetrics remote-write receiver.

- `ecr/`
  Shared ECR repositories for the app, stubs, and load-test images.

Typical first-time setup:

```sh
cp infra/aws/terraform/runner/terraform.tfvars.example infra/aws/terraform/runner/terraform.tfvars
cp infra/aws/terraform/ecr/terraform.tfvars.example infra/aws/terraform/ecr/terraform.tfvars
./infra/aws/scripts/linux/create-runner-and-ecr.sh us-east-1 dev
```

Typical perf-lab workflow after that:

```sh
./infra/aws/scripts/linux/build-and-push.sh us-east-1 dev
./infra/aws/scripts/linux/create-lab.sh us-east-1 dev default
./infra/aws/scripts/linux/run-test.sh us-east-1 dev infra/shared/test-definitions/ckc-baseline.yaml
./infra/aws/scripts/linux/destroy-lab.sh us-east-1 dev
```

The Grafana datasource keeps the `Prometheus` name and uid so existing dashboards continue to work. Lab metrics are pushed from the EKS-side Alloy agent to the runner and remain available after `destroy-lab`.
