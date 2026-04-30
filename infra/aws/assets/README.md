# AWS Assets

`infra/aws/assets` contains AWS-specific files that are copied to the runner by `update-runner`.

- `terraform/`
  Runner-side Terraform for disposable labs.

Shared Helm charts, test definitions, Grafana assets, and test orchestration live under `infra/shared`.
Runner-internal shell entrypoints live under `infra/aws/runner-internal`.

These files are not the local operator interface. The local operator interface stays in `infra/aws/scripts`.
