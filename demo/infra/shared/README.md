# Shared Infrastructure Assets

`demo/infra/shared` contains files reused across AWS lab, the internal lab, and local Docker development.

- `test-orchestration/`: Kubernetes test runner used by AWS lab flows
- `experiment_orchestration/`: shared experiment/test resolution, target planning, Kafka topic sizing, and environment-neutral orchestration
- `experiment_report/`: shared comparative experiment analysis and Markdown/SVG report generation
- `workloads/`: shared consumer profiles, reusable test definitions, and SLA profiles
- `audit/`: shared audit analyzer used by local, internal-lab, and AWS flows
- `grafana/`: shared dashboard and provisioning files
- `helm/`: shared application and stub charts with environment value profiles
- `warmup-kafka/`: pinned autonomous Kafka image, continuous protocol-v6 topic reconciliation, readiness, and standalone examples
