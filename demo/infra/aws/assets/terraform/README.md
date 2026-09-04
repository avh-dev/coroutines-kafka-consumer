# Runner Terraform Assets

`demo/infra/aws/assets/terraform` contains Terraform that is copied to the runner and applied there.

- `load-lab/`
  Disposable performance-lab infrastructure for EKS plus lab-level Kafka and Redis.
  It also creates temporary VPC peering and routes back to the runner so in-cluster observability agents can remote-write metrics to runner storage.
  Environment-independent defaults live in `variables.tf`; every experiment's generated JSON inputs select its concrete lab size and services.
