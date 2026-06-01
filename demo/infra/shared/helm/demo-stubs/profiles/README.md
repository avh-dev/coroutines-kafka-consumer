# Demo Stubs Helm Profiles

Stub behavior is configured dynamically through `POST /settings`. Helm profiles
only describe environment-specific deployment scaling:

- `internal-lab.yaml` runs one long-lived pod on the dedicated lab host.
- `aws-hpa.yaml` enables HPA for AWS load tests.
