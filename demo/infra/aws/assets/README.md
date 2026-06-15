# AWS Assets

`demo/infra/aws/assets` contains AWS-specific files that are copied to the runner by `update-aws-lab`.

- `terraform/`
  Runner-side Terraform for disposable labs.

Shared Helm charts, test definitions, Grafana assets, and test orchestration live under `demo/infra/shared`.
Runner-side shell entrypoints live under `demo/infra/aws/runner-assets`.

These files are not the local operator interface. The local operator interface stays in `demo/infra/aws/scripts`.
