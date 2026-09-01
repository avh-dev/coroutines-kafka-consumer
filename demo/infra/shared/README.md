# Shared Infrastructure Assets

`demo/infra/shared` contains files reused across AWS lab, the internal lab, and local Docker development.

- `test-orchestration/`: Kubernetes test runner used by AWS lab flows
- `experiment_orchestration/`: shared experiment/test resolution, target planning, Kafka topic sizing, and environment-neutral orchestration
- `experiment_report/`: shared comparative experiment analysis and Markdown/SVG report generation
- `workloads/`: shared consumer profiles and reusable test definitions
- `audit/`: shared audit analyzer used by local, internal-lab, and AWS flows
- `grafana/`: shared dashboard and provisioning files
- `helm/`: shared application and stub charts with environment value profiles
