# Runner Terraform Assets

`infra/aws/assets/terraform` contains Terraform that is copied to the runner and applied there.

- `load-lab/`
  Disposable performance-lab infrastructure for EKS plus lab-level Kafka and Redis.
  `profiles/*.tfvars` defines the available lab presets.
