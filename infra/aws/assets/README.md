# AWS Assets

`infra/aws/assets` contains files that are copied to the runner by `update-runner`.

- `terraform/`
  Runner-side Terraform for disposable labs.

- `helm/`
  Helm charts and deployment profiles used by test runs.

- `test-definitions/`
  YAML definitions that select deployment profiles and load-test settings.

- `runner/`
  Remote entrypoints that run on the runner itself.

These files are not the local operator interface. The local operator interface stays in `infra/aws/scripts`.
