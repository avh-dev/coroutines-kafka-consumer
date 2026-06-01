# Shared Infrastructure Assets

`demo/infra/shared` contains files reused across AWS lab, the internal lab, and local Docker development.

- `helm/`: app and stubs Helm charts plus environment-specific deployment profiles
- `test-definitions/`: YAML smoke/load definitions
- `test-orchestration/`: Kubernetes test runner used by AWS lab flows
- `grafana/`: shared dashboard and provisioning files
