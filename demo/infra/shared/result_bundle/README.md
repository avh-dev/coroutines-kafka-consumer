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
result. It contains `README.md`, one foreground `run-grafana.sh`, and four
purpose-specific directories: `report/`, `restore/`, `deployment/`, and `lab/`.
It does not expose collection manifests, controller session state, transport
markers, Terraform state, kubeconfigs, or arbitrary result-tree JSON.

`report/` is directly readable. `restore/` contains only the dashboard, Loki
JSONL, the native metrics snapshot, and the private Compose/import implementation.
`deployment/` preserves the source and resolved experiment, target definitions,
generated Kubernetes YAML, and execution commands. `lab/` explains the
environment; AWS evidence additionally contains controller commands, exact
Terraform module sources and resolved variables, its runner-side lab script,
and generated Helm values and commands when charts were used.

Extract evidence, enter its root directory, and run `./run-grafana.sh`. It
starts Grafana, Loki, and the matching Prometheus-compatible metrics engine, imports the preserved data, prints
the dashboard URL, and remains attached. Press `q` or `Ctrl-C` to stop and
remove the containers. Runtime files remain owned by the invoking user.

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
