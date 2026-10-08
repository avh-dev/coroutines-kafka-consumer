# Human-readable experiment results

Every environment publishes the same named result directory:

```text
<experiment>-<UTC timestamp>/
├── report/
│   ├── report.md
│   └── assets/
├── ckc-experiment-<experiment>-<UTC minute>-evidence.tar.gz
└── ckc-experiment-<experiment>-<UTC minute>-audit.tar.gz
```

The evidence archive has a strict whitelist and the same root name as the
result. It contains `README.md`, detached `start-grafana.sh` and
`stop-grafana.sh` lifecycle commands, and five purpose-specific directories:
`report/`, `restore/`, `deployment/`, `lab/`, and `diagnostics/`.
It does not expose collection manifests, controller session state, transport
markers, Terraform state, kubeconfigs, or arbitrary result-tree JSON.

`report/` is directly readable. `restore/` contains only the dashboard, Loki
JSONL, the native metrics snapshot, and the private Compose/import implementation.
`deployment/` preserves the source and resolved experiment, target definitions,
generated Kubernetes YAML, and execution commands. `lab/` explains the
environment; AWS evidence additionally contains controller commands, exact
Terraform module sources and resolved variables, its runner-side lab script,
and generated Helm values and commands when charts were used.

Extract evidence, enter its root directory, and run `./start-grafana.sh`. It
reports the restore phases while it starts Grafana, Loki, and the matching
Prometheus-compatible metrics engine and imports the preserved data. It then
prints the dashboard URL and leaves the containers running in the background.
Run `./stop-grafana.sh` to remove the containers. An interruption or failure
during startup removes a partially started stack. Runtime files remain owned
by the invoking user and are reused on the next start. Grafana grants the
bundle-local anonymous session the Editor role, so no login or password is
required and Explore plus ad-hoc dashboard/query editing remain available.
This does not grant Grafana server or data-source administration.

The launcher starts with host ports `3002` for Grafana and `3102` for Loki. If
either default is occupied, it selects the next available port and prints the
resolved Grafana URL. This permits different extracted bundles to run at the
same time; their Compose project names are derived from their bundle directory
names. `CKC_RESTORE_GRAFANA_PORT` and `CKC_RESTORE_LOKI_PORT` remain strict
overrides: when an explicitly requested port is occupied, startup fails with a
clear error instead of choosing another port.

Raw audit chunks are never duplicated into evidence. The independent audit
archive has the same named root and contains `README.md`, a combined
`summary.yaml`, and `runs/<run-id>/audit/`.

Text, YAML, and JSON selected for evidence are redacted and known checkout,
session, result, and internal-lab roots are replaced with portable variables.
Terraform state, `.terraform` directories, kubeconfigs, and secret directories
are always excluded.

`collect.py` still owns live Prometheus/Loki collection and `prepare.py` builds
the environment-aware dashboard. Their internal state supports finalization but
is not part of the published evidence contract.
