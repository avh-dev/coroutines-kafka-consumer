# Internal Lab

`demo/infra/internal-lab` runs the CKC demo on a dedicated Linux laptop with low overhead.

The app, demo stubs, Prometheus, and metrics-server run in k3s. The selected Kafka API broker, Redis, and Grafana run on the lab host through Docker Compose. Local state is stored under the repository root in `.demo-infra/internal-lab/`, which is ignored by Git.

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
    ckc-log-collector Deployment -> host Loki
    metrics-server for kubectl top and HPA
    ckc-external-kafka Service + Endpoints -> selected host Kafka API broker
    ckc-external-redis Service + Endpoints -> host Redis
    ckc-external-audit Service + Endpoints -> host Fluent Bit TCP collector
    ckc-external-loki Service + Endpoints -> host Loki
    ckc-external-load-test Service + Endpoints -> host load-test Prometheus endpoint

  Docker Compose
    Redpanda or Apache Kafka -> host:9092
    Redis -> host:6379
    Fluent Bit -> host:5170 TCP audit ingest, localhost:2020 health
    Loki -> host:3100 for persistent pod stdout/stderr logs
    Grafana -> host:3000 on all interfaces
    process-exporter -> host:9256 for host Kafka broker/Redis process CPU/memory

  Host runtime
    Docker build for ckc-perf/demo and ckc-perf/demo-stubs from synced dist layouts
    ckc-demo-load-test as a host Java process under /opt/ckc-lab/load-test-runtime, metrics on host:9405
```

Prometheus stays in Kubernetes so it can use Kubernetes service discovery and scrape app pods plus kubelet cAdvisor metrics. Grafana and Loki stay on the host so they do not add pod storage overhead to the Kubernetes test surface.
Prometheus scrapes internal-lab targets every 15 seconds and stores its TSDB on the lab host under `/opt/ckc-lab/prometheus`, mounted into the pod through `hostPath`, so normal pod restarts and config reloads do not wipe recent lab history. The lab keeps up to three days of metrics with a 20 GB retention-size cap.
Loki stores pod stdout/stderr logs under `/opt/ckc-lab/loki` with a three-day retention window. The log collector attaches namespace, pod, container, app, run id, profile, and test-definition labels when they are available.

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
the selected broker's advertised listener need a concrete node address for pod traffic.

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
- maps local internal-lab assets and shared test assets into responsibility-oriented lab-host directories under `/opt/ckc-lab`
- installs Docker, Helm, and k3s on the lab host
- starts k3s with `traefik`, `servicelb`, `local-storage`, and bundled `metrics-server` disabled
- deploys this lab's explicit metrics-server and Prometheus manifests
- keeps Prometheus history on a persistent lab-host directory and reloads Prometheus config during updates when possible instead of restarting it unconditionally
- starts host Apache Kafka by default, plus Redis, Fluent Bit, Loki, and Grafana
- provisions Grafana datasources and the shared `CKC Overview` dashboard
- writes `.demo-infra/internal-lab/kubeconfig.yaml`
- verifies `kubectl`, Grafana, Prometheus, Loki, and the Kafka API from the local machine

After install:

```sh
source .demo-infra/internal-lab/lab.env
kubectl --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml get nodes -o wide
curl -fsS "http://${LAB_HOST}:3000/api/health"
curl -fsS "http://${LAB_HOST}:30090/-/ready"
```

The lab host layout is grouped by runtime responsibility:

```text
/opt/ckc-lab/bin                public lab entrypoints
/opt/ckc-lab/libexec            internal shell helpers
/opt/ckc-lab/helpers            internal Python helpers, including audit analysis
/opt/ckc-lab/helm               internal-lab Helm charts and profiles
/opt/ckc-lab/docker/compose     host-service Docker Compose files
/opt/ckc-lab/docker/build       Docker build contexts
/opt/ckc-lab/k8s                raw Kubernetes manifests and templates
/opt/ckc-lab/config             persisted lab and test config
/opt/ckc-lab/grafana            Grafana provisioning and dashboards
/opt/ckc-lab/workloads          shared consumer profiles, test definitions, and experiment definitions
/opt/ckc-lab/load-test-runtime  built load-test runtime
/opt/ckc-lab/state              fingerprints, generated files, and pids
/opt/ckc-lab/logs               lab process logs
/opt/ckc-lab/results            run and experiment result artifacts
/opt/ckc-lab/prometheus         persistent Prometheus data
/opt/ckc-lab/loki               persistent Loki log data
```

## Clean Reinstall

To rebuild an existing lab host from scratch, first make sure the current
assets are present on the lab host, then run the destructive lab-side cleanup:

```sh
./demo/infra/internal-lab/scripts/update-lab.sh
source .demo-infra/internal-lab/lab.env
ssh "root@${LAB_HOST}"
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/uninstall-server.sh
```

The cleanup removes k3s, internal-lab Docker containers/images, Helm, Java 21,
Docker packages installed by the lab installer, and `/opt/ckc-lab`.
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
- internal-lab Helm charts, Grafana dashboards, test definitions, and helper scripts
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
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh
```

The runner presents fixed choices through numbered lists and prompts for scalar
values directly. Previous selections are marked as current or shown as defaults
and used when Enter is pressed:

- a load-test definition, defaulting to the previous run
- a consumer profile, defaulting to the currently deployed profile
- the host Kafka API broker implementation, defaulting to `apache-kafka`; select `redpanda` when needed
- whether business processing is enabled; select `false` for a noop run
- the base load rate and per-topic planning latency used for run-plan calculation
- the coroutine processing dispatcher when the selected profile supports one
- the shared fixed worker dispatcher thread count, only when the dispatcher is `FIXED`
- whether consumer and load-generator audit logging is enabled
- the consumer metrics implementation: `MICROMETER` or `NOOP`

For consumer profiles, `run-test.sh` generates a Helm values overlay under
`/opt/ckc-lab/state/generated/run-plan-values.yaml` before it changes the lab.
The plan uses the selected Spring profile, `base_tps`, per-topic traffic
percentages, explicit per-topic planning latency, processing-enabled mode, and
explicit processing-mode overrides to calculate topic partitions and either
worker concurrency or Spring Kafka listener concurrency. Dispatcher selection is consumer-profile scoped:
`spring-kafka`, `spring-kafka-thread-pool`, and `confluent-sync` do not expose it, `ckc-sync` exposes only
`IO` and `VIRTUAL`, and coroutine-worker profiles expose `DEFAULT`, `FIXED`,
`IO`, and `VIRTUAL`. Experiments always pass planning latency explicitly per
target. Manual `run-test.sh` runs require the same explicit latency values,
entered interactively or passed with `--order-planning-latency-ms`,
`--batch-planning-latency-ms`, and `--telemetry-planning-latency-ms`. The full
plan is printed before preparation and is included in each run's
`run-metadata.json`.

Before destructive setup starts, interactive runs ask whether to apply the plan,
edit it manually, or abort. Manual editing exposes only the knobs supported by
the selected profile and processing mode. `spring-kafka` supports partitions and
pollers; `spring-kafka-thread-pool` keeps poller concurrency low and tunes fixed
platform-thread workers; CKC and Confluent profiles normally tune workers, while partition
ordering modes also tune partitions.

Definitions with `chaos_steps`, such as `chaos-smoke`, start a separate chaos
executor after the load-test process starts. Chaos offsets are measured from
that load-test start point. The executor writes its own log under
`/opt/ckc-lab/results/runs/<run-id>/logs/chaos.log`, and the runner fails fast if a
chaos scenario fails. Instant scenarios define only their start offset:

```yaml
chaos_steps:
  - at: 3m
    type: pod_crash
    target: ckc-demo
```

Duration-based scenarios define one semantic interval instead of separate
apply/reset commands:

```yaml
chaos_steps:
  - at: 4m10s
    duration: 3m20s
    type: service_outage
    target: redis
```

Supported instant types are `pod_delete`, `pod_crash`, and `service_restart`.
Supported duration-based types are `stubs_degradation`,
`network_degradation`, and `service_outage`. The executor schedules the inverse
action at the interval end and also restores every active interval if execution
fails or receives `SIGINT`/`SIGTERM`. Preparation and run cleanup remove stale
network degradation, unpause known services, and restore the configured stubs
baseline. Overlapping duration-based scenarios for the same target are rejected
as ambiguous.

Pass the same choices explicitly for a non-interactive run:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh \
  --profile ckc-sync \
  --order-processing-mode AT_LEAST_ONCE_PARTITION_ORDERING \
  --batch-processing-mode AT_LEAST_ONCE_PARTITION_ORDERING \
  --base-rate 3000 \
  --order-planning-latency-ms 45 \
  --batch-planning-latency-ms 60 \
  --telemetry-planning-latency-ms 8 \
  --kafka-implementation apache-kafka \
  --processing-dispatcher-type VIRTUAL \
  --processing-enabled false \
  --audit-log-enabled false \
  --metrics-implementation NOOP \
  baseline
```

Use `--order-processing-mode`, `--batch-processing-mode`, and
`--telemetry-processing-mode` to override the profile defaults. Explicit modes
must be allowed by the selected profile. If the previous run used a mode that is
not valid for the new profile, the profile default is used instead. Use
`--dry-run-plan` to print the computed partitions, workers, listener
concurrency, average latency assumptions, and generated values path without
resetting Kafka or deploying the app. Non-interactive runs can override the
computed numbers directly with `--order-partitions`, `--order-workers`,
`--order-pollers`, and the equivalent `batch` and `telemetry` flags.
For non-CKC consumers, `HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED` means the demo
consumer uses its built-in stale-record filter; it is rendered to the existing
application enum value internally but shown separately in run plans and experiments
to avoid confusing it with CKC's `FRESHNESS_FIRST_DROP_OLDEST` queue-overflow
mode.

The default `apache-kafka` option runs the official `apache/kafka:4.3.1` image and
enables share groups through `group.coordinator.rebalance.protocols=classic,consumer,streams,share`.
The single-node lab also sets the share coordinator state topic replication
factor and min ISR to `1`, so share-group experiments do not wait for a
multi-broker cluster.
Redpanda remains available with `--kafka-implementation redpanda`.
For Apache Kafka runs, the lab builds the local Thread Stats Java agent from
the sibling `../thread-stats` repository, mounts it into the Kafka container,
and exposes broker thread metrics at `http://${LAB_HOST}:9404/prometheus`.
Set `THREAD_STATS_REPO` or `THREAD_STATS_AGENT_JAR` before `install-lab.sh` or
`update-lab.sh` to use a different local checkout or prebuilt agent jar. These
metrics use the Prometheus job `ckc-kafka-thread-stats`, separate from the demo
application's `/actuator/prometheus` Thread Stats metrics.

Any generated test environment value can also be overridden with repeated
`--env KEY=VALUE` flags. These overrides are applied after the consumer profile and
test definition are rendered. Static Helm profiles are no longer used for normal
test runs; `demo/infra/internal-lab/assets/helm/demo/profiles/demo.yaml`
is kept only as a manual Helm/debug overlay.

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh \
  --profile ckc \
  --processing-dispatcher-type FIXED \
  --worker-dispatcher-threads 2 \
  --base-rate 3000 \
  telemetry-freshness-fairness
```

Before starting the load generator, the runner:

- generates Kafka topics from the computed run plan
- reads mandatory stub baseline settings and load parameters from the selected lab test definition
- removes any old demo HPA and scales the demo app down so consumer groups are inactive
- resets Redis on the lab host
- deletes stale Kafka consumer groups for the demo app
- deletes and recreates Kafka topics on the selected host broker
- reuses the long-lived `ckc-demo-stubs` deployment and applies its settings through `POST /settings`
- applies the generated app Helm overlay with processing, audit logging, consumer metrics implementation, dispatcher, and worker settings overridden from the prompts

Each test definition declares the modeled peak capacity of one independently scaled producer service:

```yaml
load_test:
  producer_capacity_tps:
    order: 1000
    batch: 1000
    telemetry: 1000
```

The load-test runtime calculates each topic's Kafka producer pool from the peak local topic TPS. The runner records these capacities in `run-metadata.json`, exposes the host process on port `9405`, and Prometheus scrapes it with job `ckc-load-test`. Grafana's `Load Test: Kafka Producers` row shows pool sizes, generated and acknowledged throughput, batch size, records per request, compression ratio, queue/request latency, broker throttling, buffer utilization, retries, errors, and in-flight pressure.

To rerun only the load generator without resetting Redis, topics, or the app deployment:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh \
  --skip-prepare \
  --profile ckc-sync \
  --processing-enabled false \
  --audit-log-enabled false \
  --metrics-implementation NOOP \
  --processing-dispatcher-type IO \
  baseline
```

Even with `--skip-prepare`, stub baseline settings are applied before the load generator starts.
`--worker-dispatcher-threads` limits the shared fixed worker pool when the
selected profile uses `--processing-dispatcher-type FIXED`; it is rejected for
other dispatcher types and for profiles that do not use the demo processing
dispatcher.
The script exports `load_test` settings as environment variables for `ckc-demo-load-test`.

Run the bundled internal-lab chaos smoke explicitly with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh \
  --profile ckc \
  chaos-smoke
```

Start the experiment selector with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh
```

Or run one or more specific experiments with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh telemetry-fairness-profile-comparison ckc-sync-dispatcher-comparison
```

Compare suspend CKC, blocking CKC sync IO, and blocking CKC sync virtual-dispatcher mode with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh ckc-sync-dispatcher-comparison
```

Run every synced experiment sequentially with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh all
```

Experiment definitions live under `/opt/ckc-lab/workloads/experiments`. The
runner executes each experiment target through `run-test.sh` and writes
experiment-level logs and JSON summaries under
`/opt/ckc-lab/results/experiments/<experiment-set-id>`.
After all target audit analyses finish, the runner also generates one local
human-readable report per experiment:

```text
/opt/ckc-lab/results/experiments/<experiment-set-id>/
  summary.json
  reports/
    README.md
    <experiment-name>/
      report.md
      report-model.yaml
      load-profile.svg
      latency-sla-misses.svg
      latency-p95.svg
      cpu-average.svg
      throughput-average.svg
      raw/
```

`report-model.yaml` is the stable boundary between result analysis and
presentation. The Markdown and SVG renderers only consume that normalized
model. Reports use the audit summary for delivery correctness and exact
per-record end-to-end latency SLA evaluation. Audit latency is measured from
the Kafka record timestamp to the unique successful terminal `C` timestamp and
therefore includes backlog drained after traffic generation stops. Prometheus
is queried over each target's planned load window for diagnostic latency, CPU,
throughput, and telemetry freshness charts. Missing Prometheus data produces
explicit warnings and unavailable chart values without suppressing the audit
SLA result. The `raw` directory keeps the experiment, test, resolved SLA, run
metadata, and audit summary inputs needed when a selected report is copied into
the repository later.

Experiment definitions select a reusable SLA profile with `sla_profile`. The
profiles live under `/opt/ckc-lab/workloads/sla-profiles`. The built-in
`delivery-integrity` profile checks missing terminal records, duplicate
processing, unmatched terminal outcomes, and conflicting outcomes. The
`consumer-baseline` profile extends it with exact latency rules: business events
must complete within two seconds with at most one percent above the limit, and
telemetry must complete within one second with at most five percent above the
limit. The report presents processed records, latency violations, violation
percentage, and maximum observed latency together. Execution, delivery SLA,
latency SLA, and overall evaluation remain separate; overall PASS requires all
configured components to pass.

Regenerate reports for an existing experiment result with:

```sh
python3 /opt/ckc-lab/helpers/generate-experiment-report.py \
  /opt/ckc-lab/results/experiments/<experiment-set-id>
```

Historical audit summaries created before latency SLA support do not contain
per-record violation counts. Reanalyze their preserved compressed audit logs
with the current resolved profile before rendering:

```sh
python3 /opt/ckc-lab/helpers/generate-experiment-report.py \
  /opt/ckc-lab/results/experiments/<experiment-set-id> \
  --reanalyze-audit
```

This can take several minutes per high-volume target and replaces each run's
`audit/summary.yaml` and `audit/analyzer-progress.log` only after successful
reanalysis.

Evidence Bundle export remains a separate manual operation. Reports state that
evidence has not been exported and show the corresponding export command; they
do not publish or commit any artifact automatically. Load-profile SVGs use TPS
on the vertical axis and compact durations (`20s`, `2m`, `1h 5m`) on the horizontal axis. Phase names and
minute/second durations follow the load segments. Instant chaos scenarios use
timeline markers; duration-based scenarios use translucent ranges. Their cards
occupy separate chronological lanes below the plot and reserve independent
action and service icon slots. Stub-degradation cards also compare the configured
p90/p95/p99/p100 delay and error-rate values, omitting unaffected downstreams
and highlighting each `base → degraded` change. Time-axis labels remain below
the plot on translucent backgrounds drawn over marker lines. Optional SVG,
PNG, or WebP service artwork can be placed under
`helpers/experiment_report/icons/services`; the renderer embeds it into the
generated SVG and otherwise uses deterministic fallback badges. Recovery time is
intentionally unavailable until the lab
stores structured actual chaos-event timestamps and a recovery policy is
defined.
After the initial experiment selection and experiment-wide settings prompt,
targets run unattended. Type `q` and press Enter while an experiment is running
to ask the current target to stop, finalize its raw audit log, and abort the
rest of the experiment.

After tuning a single run with `run-test.sh`, print a target draft from the
latest completed run:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/target-draft.sh
```

Pass a run id or result run directory to use a specific run, and `--id` or
`--name` to set the target identity:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/target-draft.sh 20260713T120000Z --id ckc.fixed.8
```

For a short end-to-end experiment smoke test, run:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh smoke-repeat
```

Export the latest experiment result into a portable result directory with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/export-result.sh
```

Export a specific run or experiment result with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/export-result.sh --run 20260717T161010Z
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/export-result.sh 20260717T170000Z
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/export-result.sh --latest-experiment
```

Result directories are written under `/opt/ckc-lab/results/exports/<experiment-name>-<date>`.
Each export contains `summary.md`, `metrics-logs-<experiment-name>-<date>.tar.gz`,
and `audit-<experiment-name>-<date>.tar.gz`. The metrics/logs archive contains
the local Grafana helper scripts, dashboard JSON, prebuilt Loki data, Prometheus
query-range TSDB blocks for dashboard metrics in the selected run or experiment
time window, run metadata, and `manifest.json`. The audit archive contains
`audit/<run-id>` artifacts for each run. Use `--skip-loki` or
`--skip-prometheus` when those services are unavailable or their data is not
needed. The exported dashboard is renamed to
`CKC experiment: <experiment-name>`, defaults to the experiment time range, and
includes an experiment summary panel with `Reset time range` and `Open logs`
links for the original range.
Loki logs are ingested into a temporary Loki container during export and stored
as a ready-to-mount data directory, so export can take longer while local restore
does not need to replay log pushes.

Restore exported metrics and Loki logs locally with:

```sh
cd smoke-repeat-20260717T170000Z
tar -xzf metrics-logs-smoke-repeat-20260717T170000Z.tar.gz
cd smoke-repeat-20260717T170000Z
./open-grafana-with-logs-and-metrics.sh
```

Grafana is available at `http://localhost:3000` with `admin` / `admin` by
default. If local ports are already in use, set `GRAFANA_PORT`, `LOKI_PORT`, or
`PROMETHEUS_PORT` before starting the script. `open-grafana-with-logs-and-metrics.sh` prepares exported Prometheus and Loki data,
starts Docker Compose, prints dashboard, Prometheus, and Loki links, then waits
until `q` is pressed.
The restored Prometheus uses a short query lookback so exported timeline/info
series do not extend across target boundaries after staleness markers are lost
while rebuilding TSDB blocks from raw samples.
When `q` is pressed, the script reports shutdown progress, stops the restore
stack, and removes its Docker volumes.

Experiment-wide environment overrides can also be passed non-interactively:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh telemetry-fairness-profile-comparison \
  --env PROCESSING_ENABLED=true \
  --env AUDIT_LOG_ENABLED=true \
  --env METRICS_IMPLEMENTATION=MICROMETER
```

When audit logging is enabled, experiment runs first execute all load phases and
collect raw audit logs under `/opt/ckc-lab/results/runs/<run-id>/audit`.
Audit analysis then runs as a separate experiment phase for every completed run,
and the raw audit logs are compressed after successful analysis.
Experiment execution continues after an individual target fails, so failed run
directories, logs, metrics, and audit files remain available in the experiment
summary and result export. Pressing `q` is treated as an explicit interruption
and stops the remaining experiment targets.

Optional notification hooks live under `/opt/ckc-lab/notify`. If
`/opt/ckc-lab/notify/notify.py` or `/opt/ckc-lab/notify/notify.sh` exists and
is executable, `run-experiment.sh` calls it as:

```text
notify-hook event-name payload.json
```

Supported event names include `experiment_started`, `test_started`,
`test_finished`, `experiment_runs_finished`, `audit_analysis_started`,
`audit_analysis_finished`, `audit_run_analysis_started`,
`audit_run_analysis_finished`, `experiment_finished`, and `experiment_failed`.

The lab sync includes a Telegram example hook at
`/opt/ckc-lab/notify/notify-telegram.py` plus setup instructions in
`/opt/ckc-lab/notify/README.md`. Enable it by creating a local shell wrapper
named `/opt/ckc-lab/notify/notify.sh`; this wrapper is not synced from the repository,
so secrets stay on the server:

```sh
cat > /opt/ckc-lab/notify/notify.sh <<'EOF'
#!/usr/bin/env sh
export TELEGRAM_BOT_TOKEN='replace-me'
export TELEGRAM_CHAT_ID='replace-me'
exec /opt/ckc-lab/notify/notify-telegram.py "$@"
EOF
chmod 0750 /opt/ckc-lab/notify/notify.sh
```

`notify-telegram.py` also supports optional `TELEGRAM_THREAD_ID` and
`TELEGRAM_EVENTS` environment variables.

High-volume publish and processed audit records are streamed over TCP into the
host Fluent Bit collector. Fluent Bit writes compact audit lines into:

```text
/opt/ckc-lab/results/live/audit/audit.log
```

Audit lines use compact record types: `P` for published records, `C` for
processed records, `D` for intentional drops, `F` for terminal processing
failures, and `R` for retry attempts that are not terminal outcomes. Drop
records may include a final reason field, for example `stale_age`,
`queue_overflow`, `replaced_by_newer_key_record`, `new_key_queue_full`, or
`already_processed`.

For each single `run-test.sh` run, the script resets the live audit file before
the load generator starts, lets Fluent Bit append compact lines during the run,
then stops Fluent Bit after drain and moves the completed audit log plus the
calculated report under:

```text
/opt/ckc-lab/results/runs/<run-id>/
  run-metadata.json
  run-status.json
  logs/
    chaos.log
  audit/
    audit-<run-id>.log[.gz]
    analyzer-progress.log
    summary.yaml
  diagnostics/
    thread-stats/
      collector.log
      index.jsonl
      summary.json
      <pod-name>/
        <timestamp>.json
        <timestamp>.txt
```

While the load test is active, the runner immediately collects a full Thread
Stats actuator snapshot from every running demo pod and repeats the collection
once per minute by default. Pod discovery runs before every cycle, so restarted
pods and replica changes are reflected without restarting the collector. Each
snapshot is stored under its pod name as both structured JSON and the formatted
plain-text Thread Stats report with the same timestamp. `index.jsonl` preserves
the pod UID, collection status, artifact paths, and sizes,
while `summary.json` provides per-run and per-pod coverage totals. Collection
errors are recorded in the index and `collector.log` without stopping the load
test. Override `THREAD_STATS_SNAPSHOT_INTERVAL_SECONDS` when a different
sampling interval is required.

Experiment reports include a Thread Stats coverage table. Evidence Bundle
exports retain the complete `diagnostics` directory for every included run.

The runner prints and saves `summary.yaml` with the selected test settings,
published, processed, missing, duplicate, and ordering counts calculated from
the audit lines. After successful analysis, the raw audit file is compressed as
`audit-<run-id>.log.gz` to keep historical runs compact. The report contains
aggregate audit totals and the same full summary for each topic. Latency is
intentionally left to Prometheus and Grafana time-series metrics instead of the
static audit summary. Analyzer stderr is saved to `audit/analyzer-progress.log`
in the same run directory. Delivery
correctness uses exact offline publish-to-terminal matching by default, so
long-delayed terminal records from chaos and slow-stub runs do not get counted
as missing. During the final analysis step, the runner also prints analyzer
progress such as `files=1/1 10% records=...` to the terminal.

`run-test.sh --skip-analysis` finalizes the raw audit log and writes run
metadata/status but leaves `summary.yaml` generation for a later analyzer pass.
This is the mode used by `run-experiment.sh`.

For telemetry freshness comparisons, use the `telemetry-freshness-fairness`
test definition and select the telemetry processing mode explicitly with
`--telemetry-processing-mode`. The cauldron topic summary then includes
`key_fairness` metrics for processed/dropped ratio skew, per-key processed
gaps, and processed/dropped record age. Keep this definition on one load-test
worker unless you intentionally want one fixed fleet per worker; otherwise
active key cardinality increases and can trigger `new_key_queue_full` before
the key-coalescing behavior is measured.

After the local generator exits, the runner waits for Prometheus
`kafka_consumergroup_lag{consumergroup="ckc-demo"}` to drain to zero
and stay there briefly before running audit analysis and printing the final
summary. Audit analysis is intentionally not run in parallel with the load test
so local CPU contention does not affect benchmark results.
If the Kafka exporter metrics are temporarily unavailable, the runner falls back
to the selected broker's host admin CLI: `rpk group describe` for Redpanda or
`kafka-consumer-groups.sh --describe` for Apache Kafka.
When audit logging is enabled, the runner also fails fast if the Fluent Bit
collector health endpoint is unavailable before the test starts or during the
load run. Override the lag wait with:

```sh
CONSUMER_DRAIN_TIMEOUT_SECONDS=1800 LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh --skip-drain-wait
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
Loki:       http://$LAB_HOST:3100
Kafka API:  $LAB_HOST:9092
Redis:      $LAB_HOST:6379
Audit TCP:  $LAB_HOST:5170
```

Grafana credentials are `admin` / `admin`.

The shared dashboard is provisioned under the `CKC` folder:

```text
http://$LAB_HOST:3000/d/ckc-overview/ckc-overview
```

Dashboard JSON is mounted from `/opt/ckc-lab/grafana/dashboards`,
the same path refreshed by `update-lab.sh`. The update script reapplies the Grafana compose service so mount
changes and refreshed dashboard JSON are visible without manual copying.

Grafana also provisions a `Loki` datasource for pod logs. Use Explore with LogQL, for example:

```logql
{namespace="ckc-perf", app="ckc-demo"}
{namespace="ckc-perf", app="ckc-demo", run_id="20260717T120000Z"}
{namespace="ckc-perf", pod=~"ckc-demo-.*"} |= "ERROR"
```

The `run_id`, `profile`, and `test_definition` labels are set on `ckc-demo` pods during `run-test.sh` preparation, so logs remain queryable after pod restarts as long as they were collected before the pod disappeared.

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
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh
```

While topics are being deleted and recreated, running app pods can briefly log `UNKNOWN_TOPIC_OR_PARTITION`. That should stop after `prepare-test` finishes and the topics exist again.

Apply the generic manual/debug Helm overlay without changing the generated run
profile model:

```sh
helm upgrade --install ckc-demo demo/infra/internal-lab/assets/helm/demo \
  --kubeconfig .demo-infra/internal-lab/kubeconfig.yaml \
  --namespace ckc-perf \
  -f demo/infra/internal-lab/assets/config/demo-values.yaml \
  -f demo/infra/internal-lab/assets/helm/demo/profiles/demo.yaml
```

Useful Prometheus queries:

```promql
sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="ckc-perf", container="demo"}[30s]))
sum by (pod) (container_memory_working_set_bytes{namespace="ckc-perf", container="demo"})
rate(ckc_poll_duration_seconds_count[30s])
sum by (consumer_impl, consumer_id) (rate(ckc_poll_records_sum[30s]))
sum(rate(ckc_poll_records_sum[30s])) by (consumer_impl, consumer_id)
sum(rate(ckc_processing_duration_seconds_count[30s])) by (consumer_impl, consumer_id)
sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup="ckc-demo"})
sum by (traffic_topic) (ckc_load_test_producer_pool_size{job="ckc-load-test"})
avg by (traffic_topic) (kafka_producer_batch_size_avg{job="ckc-load-test"})
avg by (traffic_topic) (kafka_producer_compression_rate_avg{job="ckc-load-test"})
```

Use `kubectl top pods` for quick current snapshots. Use Prometheus/Grafana for profile comparisons because they preserve history and allow identical measurement windows.

The host-managed Kafka broker and Redis run outside Kubernetes. Broker CPU/RSS and Redis CPU/RSS are scraped through the internal-lab process exporter rather than Kubernetes cAdvisor. The same exporter discovers the demo JVM through the host process table so voluntary and nonvoluntary context-switch rates can be compared across consumer profiles:

```promql
1000 * sum by (groupname) (rate(namedprocess_namegroup_cpu_seconds_total{job="ckc-host-process-exporter", groupname=~"redpanda|apache-kafka"}[30s]))
sum by (groupname) (namedprocess_namegroup_memory_bytes{job="ckc-host-process-exporter", groupname=~"redpanda|apache-kafka", memtype="resident"})
1000 * sum by (groupname) (rate(namedprocess_namegroup_cpu_seconds_total{job="ckc-host-process-exporter", groupname="redis"}[30s]))
sum by (groupname) (namedprocess_namegroup_memory_bytes{job="ckc-host-process-exporter", groupname="redis", memtype="resident"})
sum by (ctxswitchtype) (rate(namedprocess_namegroup_thread_context_switches_total{job="ckc-host-process-exporter", groupname="ckc-demo"}[30s]))
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
curl -fsS "http://${LAB_HOST}:9405/metrics" | head
```

Host checks:

```sh
ssh "root@${LAB_HOST}" "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
ssh "root@${LAB_HOST}" "docker exec ckc-perf-redis redis-cli PING"
ssh "root@${LAB_HOST}" "docker exec ckc-perf-redpanda rpk -X brokers=localhost:9092 topic list"
# For an Apache Kafka run:
ssh "root@${LAB_HOST}" "docker exec ckc-perf-kafka env KAFKA_OPTS= /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
ssh "root@${LAB_HOST}" "curl -fsS http://127.0.0.1:9308/metrics | grep kafka_consumergroup_lag | head"
ssh "root@${LAB_HOST}" "curl -fsS http://127.0.0.1:2020/api/v1/health"
```

## Benchmark Stability

Recommended before serious runs:

```sh
source .demo-infra/internal-lab/lab.env
ssh "root@${LAB_HOST}" "LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/tune-host.sh"
ssh "root@${LAB_HOST}"
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-test.sh
```

Use wired Ethernet when possible. Run a warm-up before measuring. Keep Prometheus scrape interval at `15s` or slower unless short spike visibility is required.

The default demo app values set memory limits but do not set CPU limits. CPU limits can introduce CFS throttling and distort latency measurements. Add CPU limits only when testing constrained CPU behavior.

## Typical Pitfalls

- The selected broker's advertised Kafka address points at `localhost`.
- The app uses `localhost` for Redis, Kafka, or stubs from inside Kubernetes.
- Images were rebuilt locally but not loaded into k3s.
- Kafka topics were not recreated between tests.
- CPU limits cause throttling and look like application latency.
- The lab has not been refreshed with `update-lab.sh` after local code changes.
- Prometheus scrape interval is too aggressive.
- Swap is enabled or CPU governor is `powersave`.
- Windows scripts are run from PowerShell instead of Git Bash.
