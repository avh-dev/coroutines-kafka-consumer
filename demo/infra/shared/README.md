# Shared Infrastructure Assets

`demo/infra/shared` contains files reused across AWS lab, local Docker development, and local Kubernetes smoke tests.

- `helm/`: app and stubs Helm charts plus deployment profiles
- `test-definitions/`: YAML smoke/load definitions
- `test-orchestration/`: Kubernetes test runner used by AWS and local k8s flows
- `grafana/`: shared dashboard and provisioning files
