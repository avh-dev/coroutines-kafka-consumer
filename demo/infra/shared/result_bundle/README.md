# Canonical experiment artifacts

Every environment publishes the same three logical artifacts:

- `report.md` and its `report-assets/` image directory;
- `evidence.tar.gz`, rooted at `evidence/`;
- `audit.tar.gz`, rooted at `audit/`.

Both archives contain a versioned `manifest.json` with SHA-256 and size for each
member. The evidence manifest also records the SHA-256 of the independent audit
archive. A finalization failure leaves no `*.partial` files; completed outputs
replace previous files only after all new artifacts have been staged.

`evidence/result/` contains the resolved definitions, metadata, metrics, logs,
events, analyzer summaries, generated deployment inputs, dashboards, and
environment description available at finalization time. Raw audit streams live
only in `audit/runs/<run-id>/audit/`; audit summaries remain in evidence for
ordinary report inspection. `evidence/restore/` is the environment-independent
offline restore kit.

Terraform state, `.terraform` directories, kubeconfigs, and secret directories
are excluded. Values beneath JSON/YAML keys that look like credentials, tokens,
passwords, secrets, access keys, or private keys are replaced with
`<redacted>`. Common text assignment forms and AWS access-key identifiers are
redacted as well. The raw audit archive is copied byte-for-byte and therefore
must not contain credentials at collection time.

The finalizer accepts `complete`, `failed`, or `interrupted` status. When report
generation did not run, it emits a small failure report that points to the
collected diagnostics instead of omitting the artifact contract.

`collect.py` is the shared live-source collector. Environment adapters supply
only Prometheus and Loki endpoints plus the run directories; the collector
writes canonical Loki JSONL, Prometheus TSDB blocks, and a collection manifest.
Source failures are recorded in that manifest so diagnostics can still be
finalized. `prepare.py` then builds the same environment-aware dashboard and
archived-file log stream for either environment.

The only restore implementation is `result_bundle/restore`. Its Compose file,
pinned images, Grafana provisioning, and import helpers are copied unchanged
into every evidence archive. Environment adapters do not package their own
restore scripts.
