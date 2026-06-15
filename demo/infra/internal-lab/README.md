# Internal Lab

`demo/infra/internal-lab` runs the CKC demo on a dedicated Linux laptop with low overhead.

The app, demo stubs, Prometheus, and metrics-server run in k3s. Redpanda, Redis, and Grafana run on the lab host through Docker Compose. Local state is stored under the repository root in `.demo-infra/internal-lab/`, which is ignored by Git.

## Requirements

- Lab host: Linux, preferably Ubuntu or Debian.
- Local machine: `root` SSH access to the lab host through a key.
- Local tools: Git Bash-compatible shell, `ssh`, `scp`, `rsync` when available, `kubectl`, `helm`, Java/Gradle.
- Windows users should run all local scripts from Git Bash, not PowerShell.

The lab host is stored in `.demo-infra/internal-lab/lab.env`. Repository scripts and manifests should not hardcode a lab IP. Prefer a stable hostname such as `optilab` in the local hosts file; `install-lab.sh` resolves it to the current lab IP for Kubernetes endpoints.

## Architecture

```text
local machine
  .demo-infra/internal-lab/kubeconfig.yaml -> k3s API on lab host
  install-lab.sh -> base host setup
  update-lab.sh -> local Gradle build, artifact sync, lab-side image rebuild

lab host
  k3s
    namespace ckc-perf
    ckc-demo Deployment, NodePort 30080
    ckc-demo-stubs Deployment, ClusterIP 8080
    ckc-prometheus Deployment, NodePort 30090
    metrics-server for kubectl top and HPA
    ckc-external-kafka Service + Endpoints -> host Redpanda
    ckc-external-redis Service + Endpoints -> host Redis
    ckc-external-audit Service + Endpoints -> host Fluent Bit TCP collector

  Docker Compose
    Redpanda -> host:9092
    Redis -> host:6379
    Fluent Bit -> host:5170 TCP audit ingest, localhost:2020 health
    audit-archiver -> completed gzip audit chunks under /opt/ckc-internal-lab/audit/live/chunks
    Grafana -> host:3000 on all interfaces
    process-exporter -> host:9256 for host Redpanda/Redis process CPU/memory

  Host runtime
    Docker build for ckc-perf/demo and ckc-perf/demo-stubs from synced dist layouts
    ckc-demo-load-test as a host Java process under /opt/ckc-internal-lab/runtime/load-test
```

Prometheus stays in Kubernetes so it can use Kubernetes service discovery and scrape app pods plus kubelet cAdvisor metrics. Grafana stays on the host so it does not add pod overhead to the Kubernetes test surface.
Prometheus stores its TSDB on the lab host under `/opt/ckc-internal-lab/prometheus`, mounted into the pod through `hostPath`, so normal pod restarts and config reloads do not wipe recent lab history.

## Install

From Git Bash at the repository root:

```sh
./demo/infra/internal-lab/scripts/install-lab.sh
```

On the first run, the script asks for the lab host name or IP and writes:

```text
.demo-infra/internal-lab/lab.env
```

For example, add the lab Wi-Fi address to your local hosts file:

```text
192.168.1.50 optilab
```

Then answer `optilab` when `install-lab.sh` prompts. The only local lab state
value is `LAB_HOST=optilab`; scripts derive `root@optilab` for SSH. The
resolved IP is written only into the lab-host config because k3s Endpoints and
Redpanda's advertised listener need a concrete node address for pod traffic.

You can pre-create or edit the state file instead of using the prompt:

```sh
mkdir -p .demo-infra/internal-lab
cat > .demo-infra/internal-lab/lab.env <<'EOF'
LAB_HOST=optilab
EOF
./demo/infra/internal-lab/scripts/install-lab.sh
```

The installer:

- writes or refreshes `.demo-infra/internal-lab/lab.env`
- copies `demo/infra/internal-lab/assets` to `/opt/ckc-internal-lab/assets` on the lab host
- installs Docker, Helm, and k3s on the lab host
- starts k3s with `traefik`, `servicelb`, `local-storage`, and bundled `metrics-server` disabled
- deploys this lab's explicit metrics-server and Prometheus manifests
- keeps Prometheus history on a persistent lab-host directory and reloads Prometheus config during updates when possible instead of restarting it unconditionally
- starts host Redpanda, Redis, Fluent Bit, and Grafana
- provisions Grafana datasource and the shared `CKC Overview` dashboard
- writes `.demo-infra/internal-lab/kubeconfig.yaml`
- verifies `kubectl`, Grafana, Prometheus, and the Redpanda Kafka API from the local machine

After install:

```sh
source .demo-infra/internal-lab/lab.env
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml get nodes -o wide
curl -fsS "http://${LAB_HOST}:3000/api/health"
curl -fsS "http://${LAB_HOST}:30090/-/ready"
```

## Clean Reinstall

To rebuild an existing lab host from scratch, first make sure the current
assets are present on the lab host, then run the destructive lab-side cleanup:

```sh
./demo/infra/internal-lab/scripts/update-lab.sh
source .demo-infra/internal-lab/lab.env
ssh "root@${LAB_HOST}"
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/uninstall-server.sh
```

The cleanup removes k3s, internal-lab Docker containers/images, Helm, Java 21,
Docker packages installed by the lab installer, and `/opt/ckc-internal-lab`.
After it finishes, run a fresh install from the local machine:

```sh
./demo/infra/internal-lab/scripts/install-lab.sh
```

## Update Lab

Build and sync only the lab pieces whose fingerprints changed, then rebuild or redeploy only the affected runtime parts on the lab host:

```sh
./demo/infra/internal-lab/scripts/update-lab.sh
```

This keeps uncommitted local code changes testable without making the lab host run Gradle or read from Git. The update step tracks independent fingerprints for:

- `ckc-demo` and `ckc-demo-stubs` image build inputs
- the `ckc-demo-load-test` runtime distribution
- shared Helm charts, Grafana dashboards, test definitions, and helper scripts
- internal-lab assets and host scripts

This means a `test-definition` or Helm-only change usually skips Gradle, image rebuilds, and the base lab redeploy. Changes to Grafana or internal-lab k8s or compose assets still reapply the base lab, and demo-stubs chart changes still re-run the stubs deployment.

Force a rebuild even when the fingerprint matches:

```sh
./demo/infra/internal-lab/scripts/update-lab.sh --force-rebuild
```

## Run A Test

After `update-lab.sh`, connect to the lab host:

```sh
source .demo-infra/internal-lab/lab.env
ssh "root@${LAB_HOST}"
```

Start the interactive runner:

```sh
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh
```

The runner presents fixed choices through numbered lists and prompts for the
worker thread count directly. Previous selections are marked as current or
shown as defaults and used when Enter is pressed:

- a Helm deployment profile, defaulting to the currently deployed profile
- whether business processing is enabled; select `false` for a noop run
- whether consumer and load-generator audit logging is enabled
- the consumer metrics implementation: `MICROMETER` or `NOOP`
- the shared suspend worker dispatcher thread count
- a load-test definition, defaulting to the previous run

Pass the same choices explicitly for a non-interactive run:

```sh
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh \
  --deployment ckc-sync \
  --processing-enabled false \
  --audit-log-enabled false \
  --metrics-implementation NOOP \
  --worker-dispatcher-threads 8 \
  baseline
```

Before starting the load generator, the runner:

- reads Kafka topics from `lab.kafkaTopics` in the selected Helm deployment profile
- reads mandatory stub baseline settings and load parameters from the selected lab test definition
- removes any old demo HPA and scales the demo app down so consumer groups are inactive
- resets Redis on the lab host
- deletes stale Kafka consumer groups for the demo app
- deletes and recreates Redpanda topics on the lab host
- reuses the long-lived `ckc-demo-stubs` deployment and applies its settings through `POST /settings`
- applies the selected app Helm profile with processing, audit logging, consumer metrics implementation, and suspend worker dispatcher threads overridden from the prompts

To rerun only the load generator without resetting Redis, topics, or the app deployment:

```sh
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh \
  --skip-prepare \
  --deployment ckc-sync \
  --processing-enabled false \
  --audit-log-enabled false \
  --metrics-implementation NOOP \
  --worker-dispatcher-threads 8 \
  baseline
```

Even with `--skip-prepare`, stub baseline settings are applied before the load generator starts.
`--worker-dispatcher-threads` limits the shared fixed worker pool for the
suspend `ckc` and `confluent-parallel-reactor` profiles. The blocking `ckc-sync`
profile continues to use `Dispatchers.IO`.
The script exports `load_test` settings as environment variables for `ckc-demo-load-test` and redirects stdout/stderr to:

```text
/opt/ckc-internal-lab/logs/
```

High-volume publish and processed audit records are streamed over TCP into the
host Fluent Bit collector. Fluent Bit forwards parsed records to the local
audit archiver, which writes completed gzip chunks into:

```text
/opt/ckc-internal-lab/audit/live/chunks/*.log.gz
```

For each run, `run-test.sh` resets the live chunk directory before the load
generator starts, lets the collector and archiver write completed chunks during
the run, then stores the completed chunks plus the calculated report under:

```text
/opt/ckc-internal-lab/audit/<run-id>/
```

The runner prints and saves `summary.yaml` with the selected test settings,
published, processed, missing, duplicate, and ordering counts calculated from
the archived audit lines. The report contains aggregate audit totals and the
same full summary for each topic. Latency is intentionally left to Prometheus
and Grafana time-series metrics instead of the static audit summary. Analyzer
stderr is saved to `analyzer-progress.log` in the same run directory. During the
final analysis step, the runner also prints analyzer progress such as
`chunks=12/40 records=...` to the terminal.

For telemetry freshness comparisons, use the `telemetry-freshness-fairness`
test definition with `ckc-telemetry-freshness-first`,
`ckc-telemetry-freshness-first-by-key`, and
`spring-kafka-coroutines-naive-telemetry-threshold`. The cauldron topic summary
then includes `key_fairness` metrics for processed/dropped ratio skew,
per-key processed gaps, and processed/dropped record age. Keep this definition
on one load-test worker unless you intentionally want one fixed fleet per
worker; otherwise active key cardinality increases and can trigger
`new_key_queue_full` before the key-coalescing behavior is measured.

After the local generator exits, the runner waits for Prometheus
`kafka_consumergroup_lag{consumergroup=~"potion-tracking-.*"}` to drain to zero
and stay there briefly before running audit analysis and printing the final
summary. Audit analysis is intentionally not run in parallel with the load test
so local CPU contention does not affect benchmark results.
If the Kafka exporter metrics are temporarily unavailable, the runner falls back
to `rpk group describe` against the host Redpanda broker so drain waiting still
works after the Kafka-to-Redpanda migration.
When audit logging is enabled, the runner also fails fast if the Fluent Bit
collector health endpoint is unavailable before the test starts or during the
load run. Override the lag wait with:

```sh
CONSUMER_DRAIN_TIMEOUT_SECONDS=1800 LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh --skip-drain-wait
```

It prints the Java process PID and waits until the load-test process exits. In an interactive terminal, press `q` to stop the local load-test process early.

```sh
Press q to stop the test early.
```

## Endpoints

Use `.demo-infra/internal-lab/lab.env` for the actual host:

```text
App:        http://$LAB_HOST:30080
Prometheus: http://$LAB_HOST:30090
Grafana:    http://$LAB_HOST:3000
Kafka API:  $LAB_HOST:9092
Redis:      $LAB_HOST:6379
Audit TCP:  $LAB_HOST:5170
```

Grafana credentials are `admin` / `admin`.

The shared dashboard is provisioned under the `CKC` folder:

```text
http://$LAB_HOST:3000/d/ckc-overview/ckc-overview
```

Dashboard JSON is mounted from `/opt/ckc-internal-lab/workspace/demo/infra/shared/grafana/dashboards`,
the same path refreshed by `update-lab.sh`. The update script reapplies the Grafana compose service so mount
changes and refreshed dashboard JSON are visible without manual copying.

## Scaling

Manual scaling does not need metrics-server:

```sh
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml -n ckc-perf scale deployment ckc-demo --replicas=3
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml -n ckc-perf rollout status deployment/ckc-demo
```

`kubectl top` needs metrics-server:

```sh
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml top nodes
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml -n ckc-perf top pods
```

Internal-lab comparison profiles use fixed replicas. HPA changes pod count during
the run and makes profile comparisons harder.

## Comparing Consumer Modes

Keep these constant between runs:

- Kafka topic partition counts
- app replicas
- app resource requests and limits
- stubs latency/error profile
- load profile
- warm-up and measurement window

Reset state and deploy the same profile before every run:

```sh
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh
```

While topics are being deleted and recreated, running app pods can briefly log `UNKNOWN_TOPIC_OR_PARTITION`. That should stop after `prepare-test` finishes and the topics exist again.

Switch only the app profile or explicit Helm settings. For example:

```sh
helm upgrade --install ckc-demo demo/infra/internal-lab/helm/demo \
  --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml \
  --namespace ckc-perf \
  -f demo/infra/internal-lab/assets/config/demo-values.yaml \
  -f demo/infra/internal-lab/helm/demo/profiles/internal-lab/ckc.yaml
```

```sh
helm upgrade --install ckc-demo demo/infra/internal-lab/helm/demo \
  --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml \
  --namespace ckc-perf \
  -f demo/infra/internal-lab/assets/config/demo-values.yaml \
  -f demo/infra/internal-lab/helm/demo/profiles/internal-lab/spring-kafka.yaml
```

Useful Prometheus queries:

```promql
sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="ckc-perf", container="demo"}[30s]))
sum by (pod) (container_memory_working_set_bytes{namespace="ckc-perf", container="demo"})
rate(ckc_poll_duration_seconds_count[30s])
sum by (consumer_impl, consumer_id) (rate(ckc_poll_records_sum[30s]))
sum(rate(ckc_poll_records_sum[30s])) by (consumer_impl, consumer_id)
sum(rate(ckc_processing_duration_seconds_count[30s])) by (consumer_impl, consumer_id)
sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"potion-tracking-.*"})
```

Use `kubectl top pods` for quick current snapshots. Use Prometheus/Grafana for profile comparisons because they preserve history and allow identical measurement windows.

Host-managed Redpanda and Redis run outside Kubernetes. Redpanda CPU is scraped from Redpanda public metrics; Redis CPU and host-service memory are scraped through the internal-lab process exporter rather than Kubernetes cAdvisor:

```promql
1000 * sum(rate(redpanda_cpu_busy_seconds_total{job="ckc-redpanda-public-metrics"}[30s]))
1000 * sum by (groupname) (rate(namedprocess_namegroup_cpu_seconds_total{job="ckc-host-process-exporter", groupname="redis"}[30s]))
sum(redpanda_memory_allocated_memory{job="ckc-redpanda-public-metrics"})
sum by (groupname) (namedprocess_namegroup_memory_bytes{job="ckc-host-process-exporter", groupname="redis", memtype="resident"})
```

## Verification

```sh
source .demo-infra/internal-lab/lab.env
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml -n ckc-perf get pods,svc,endpoints -o wide
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml -n ckc-perf top pods
curl -fsS "http://${LAB_HOST}:30080/actuator/health"
curl -fsS "http://${LAB_HOST}:30080/actuator/prometheus" | head
curl -fsS "http://${LAB_HOST}:30090/api/v1/targets"
curl -fsS "http://${LAB_HOST}:9256/metrics" | head
```

Host checks:

```sh
ssh "root@${LAB_HOST}" "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
ssh "root@${LAB_HOST}" "docker exec ckc-perf-redis redis-cli PING"
ssh "root@${LAB_HOST}" "docker exec ckc-perf-redpanda rpk -X brokers=localhost:9092 topic list"
ssh "root@${LAB_HOST}" "curl -fsS http://127.0.0.1:9308/metrics | grep kafka_consumergroup_lag | head"
ssh "root@${LAB_HOST}" "curl -fsS http://127.0.0.1:2020/api/v1/health"
```

## Benchmark Stability

Recommended before serious runs:

```sh
source .demo-infra/internal-lab/lab.env
ssh "root@${LAB_HOST}" "LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/tune-host.sh"
ssh "root@${LAB_HOST}"
LAB_ROOT=/opt/ckc-internal-lab /opt/ckc-internal-lab/assets/bin/run-test.sh
```

Use wired Ethernet when possible. Run a warm-up before measuring. Keep Prometheus scrape interval at `5s` or slower unless short spike visibility is required.

The default demo app values set memory limits but do not set CPU limits. CPU limits can introduce CFS throttling and distort latency measurements. Add CPU limits only when testing constrained CPU behavior.

## Typical Pitfalls

- Redpanda's advertised Kafka address points at `localhost`.
- The app uses `localhost` for Redis, Kafka, or stubs from inside Kubernetes.
- Images were rebuilt locally but not loaded into k3s.
- Kafka topics were not recreated between tests.
- CPU limits cause throttling and look like application latency.
- The lab has not been refreshed with `update-lab.sh` after local code changes.
- Prometheus scrape interval is too aggressive.
- Swap is enabled or CPU governor is `powersave`.
- Windows scripts are run from PowerShell instead of Git Bash.
