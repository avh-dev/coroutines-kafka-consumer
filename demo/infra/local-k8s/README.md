# Local Kubernetes Tests

`demo/infra/local-k8s` runs shared Kubernetes test definitions on minikube.

It installs:

- Kafka and Redis in `ckc-app`
- Prometheus and Grafana in `ckc-observability`
- Kafka exporter in `ckc-observability` for consumer-group lag metrics
- Fluent Bit in `ckc-observability` for audit log archiving
- shared app/stubs Helm releases during a test run
- the load-test generator as a Kubernetes Job

Local Kubernetes manifests live in `demo/infra/local-k8s/manifests`.
Config files and Helm values used by those manifests live in `demo/infra/local-k8s/config`.
Small cross-platform setup helpers live in `demo/infra/local-k8s/scripts/helpers`.

Prerequisites:

- Docker
- minikube
- kubectl
- Helm
- Terraform
- Python 3

Create or refresh the local lab:

```powershell
./demo/infra/local-k8s/scripts/windows/create-lab.ps1
```

```sh
./demo/infra/local-k8s/scripts/linux/create-lab.sh
```

The default local lab setup flushes Redis, reads `demo/infra/shared/test-definitions/ckc-baseline-local.yaml`, and recreates Kafka topics from `deployment.kafka_topics`.
To prepare the lab for another definition before running it:

```powershell
./demo/infra/local-k8s/scripts/windows/create-lab.ps1 -TestDefinitionPath demo/infra/shared/test-definitions/smoke-test.yaml
```

```sh
./demo/infra/local-k8s/scripts/linux/create-lab.sh local minikube .ckc-runner/local-k8s false demo/infra/shared/test-definitions/smoke-test.yaml
```

Run the default local baseline:

```powershell
./demo/infra/local-k8s/scripts/windows/run-test.ps1
```

```sh
./demo/infra/local-k8s/scripts/linux/run-test.sh
```

The default local definition is `demo/infra/shared/test-definitions/ckc-baseline-local.yaml`.
It targets roughly 1k generated messages per second at peak across two load-test shards.

Run a specific shared test definition:

```powershell
./demo/infra/local-k8s/scripts/windows/run-test.ps1 -TestDefinitionPath demo/infra/shared/test-definitions/smoke-test.yaml
```

```sh
./demo/infra/local-k8s/scripts/linux/run-test.sh local demo/infra/shared/test-definitions/smoke-test.yaml
```

Open Prometheus and Grafana:

```powershell
./demo/infra/local-k8s/scripts/windows/open-observability.ps1
```

```sh
./demo/infra/local-k8s/scripts/linux/open-observability.sh
```

Then use:

- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3001` with `admin` / `admin`

The local runner leaves the demo, stubs, and load-test job in the cluster so recent topic-tagged application metrics remain visible after the run.
Prometheus scrapes `ckc-demo` pods through Kubernetes discovery, so app metrics include pod-level labels such as `pod`, `namespace`, and `node`.
Prometheus also scrapes `ckc-kafka-exporter`, which exposes Kafka consumer lag metrics such as `kafka_consumergroup_lag` labelled by `consumergroup`, `topic`, and `partition`.

Fluent Bit tails Kubernetes container logs from `/var/log/containers/*.log`, keeps only audit records that start with `PUBL` or `PROC`, and writes them to `/tmp/ckc-log-archive/audit.log`.
Inspect the audit archive with:

```powershell
minikube -p minikube ssh -- sudo tail -100 /tmp/ckc-log-archive/audit.log
```

```sh
minikube -p minikube ssh -- sudo tail -100 /tmp/ckc-log-archive/audit.log
```

Cleanup:

```powershell
./demo/infra/local-k8s/scripts/windows/destroy-lab.ps1
```

```sh
./demo/infra/local-k8s/scripts/linux/destroy-lab.sh
```
