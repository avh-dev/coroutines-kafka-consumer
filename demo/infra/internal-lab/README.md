# Internal lab

The internal lab is the persistent k3s execution adapter for canonical CKC
experiments. Kafka, Redis, VictoriaMetrics, Loki, Grafana, and the audit receiver
remain installed between runs; experiment-owned application and stubs resources
are generated and replaced for each target.

## Install and update

This checkout runs directly on the Linux host `optilab`. Install the lab once:

```bash
demo/infra/internal-lab/scripts/install-lab.sh
```

Synchronize shared orchestration, dashboards, reporting, experiments, and changed
images after repository updates:

```bash
demo/infra/internal-lab/scripts/update-lab.sh
```

The installed root defaults to `/opt/ckc-lab`. Configuration is under `config`,
service logs under `logs`, and run results under `results`.

## Run an experiment

Always start from the repository-level command and one self-contained experiment
file:

```bash
demo/infra/run-experiment.sh \
  demo/infra/experiments/smoke-repeat.yaml \
  --environment internal-lab
```

When the installed result directory requires root access, the shared adapter
uses one bounded non-interactive SSH command to `root@optilab`. The scripts under
`assets/bin` and `assets/libexec` consume generated target files internally;
they are not additional workload configuration interfaces.

An experiment contains:

- workload, stubs, chaos, and diagnostics;
- acceptance criteria and latency rules;
- inline implementation profiles and target overrides;
- the fixed internal-lab environment configuration.

Changing paid or fixed lab capacity between targets is intentionally unsupported.
Use separate experiment files when Kafka, Redis, or node capacity differs.

## Results

Every experiment finalizes the same named result layout as AWS:

- `<experiment>-<UTC timestamp>/report/report.md` and `report/assets/`;
- `ckc-experiment-<experiment>-<UTC minute>-evidence.tar.gz`;
- `ckc-experiment-<experiment>-<UTC minute>-audit.tar.gz`.

Evidence contains only the human-readable report, offline restore data, resolved
deployment inputs and commands, generated Kubernetes manifests, and a concise
description of the pre-existing lab. Raw compact audit streams and analyzer
output exist only in the independent audit archive.

Export an older run again with the same contract:

```bash
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/export-result.sh --run <run-id>
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/export-result.sh --experiment <experiment-set-id>
```

The default export location is
`/opt/ckc-lab/results/exports/<experiment>-<UTC timestamp>`.

## Offline restore

The evidence archive uses pinned Grafana, Loki, and Prometheus images:

```bash
tar -xzf ckc-experiment-smoke-repeat-20260905T0441Z-evidence.tar.gz
cd ckc-experiment-smoke-repeat-20260905T0441Z
./run-grafana.sh
```

Grafana is available at `http://127.0.0.1:3002`. Set
`CKC_RESTORE_GRAFANA_PORT` to use another port. Press `q` or `Ctrl-C` to stop
the stack.

## Verification

After shared orchestration, reporting, dashboard, audit, or bundle changes:

1. run the internal-lab unit tests;
2. update the installed lab;
3. run `smoke-repeat`;
4. verify every target status and acceptance result;
5. verify application Loki streams and their `app`, `pod`, `profile`, `run_id`,
   and workload labels;
6. verify Kafka exporter metrics and inspect the generated report;
7. open the produced evidence through the shared offline restore kit.

For a non-interactive privileged smoke run:

```bash
ssh -o BatchMode=yes root@optilab \
  'LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh smoke-repeat'
```

## Diagnostics

Useful checks:

```bash
kubectl -n ckc-perf get pods
kubectl -n ckc-perf get deploy ckc-demo -o yaml
curl -fsS http://127.0.0.1:30090/health
curl -fsS http://127.0.0.1:3100/ready
find /opt/ckc-lab/results/runs -path '*/audit/summary.yaml' -printf '%h\n' | sort | tail
```

Completed audit data lives under
`/opt/ckc-lab/results/runs/<run-id>/audit`. Interpret delivery correctness using
`missing_terminal`, `duplicates`, and `without_publish`; verify the selected
implementation and processing mode before attributing ordering results to the
consumer library.
