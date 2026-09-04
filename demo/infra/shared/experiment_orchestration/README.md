# Canonical experiment contract

An experiment is the only author-maintained workload document. Schema version 1
keeps the workload, acceptance rules, target configurations, and every supported
environment together. It does not permit `test_definition`, `test.extends`,
`sla_profile`, or environment overrides outside the document.

See [`experiment.schema.json`](experiment.schema.json) for the structural schema
and [`examples/portable-smoke.yaml`](examples/portable-smoke.yaml) for a complete
example. Validate and render the environment-specific immutable snapshot with:

```bash
python3 demo/infra/shared/validate-experiment.py \
  demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml \
  --environment internal-lab \
  --output /tmp/resolved-experiment.yaml
```

When more than one environment is declared, selection is mandatory. Resolution
copies only the selected environment into the snapshot, recursively applies
experiment defaults to each target, and expands every target workload override.
The original source and this `resolved-experiment.yaml` snapshot form the desired
configuration evidence for later execution tasks.

Environment adapters advertise capabilities to the resolver. Diagnostics and
chaos requested by either the base workload or a target override are rejected
before provisioning when the selected adapter does not support them. The built-in
contract currently records internal-lab chaos and packet capture support, while
AWS supports packet capture but not chaos.

Existing experiments continue through the legacy resolver during migration. Its
external test definitions and optional SLA profiles are resolved into the same
in-memory model, but new canonical documents cannot reference either catalog.

Environment configuration contains reproducibility inputs such as region,
service topology, and capacity. Credentials, account tokens, kubeconfigs, and
other operator secrets remain external runtime inputs and must not be committed
to an experiment.

Canonical target materialization also writes `deployment-plan.yaml`. The plan
contains the resolved project workload configuration, the shared planner result,
and any pinned third-party chart releases without temporary checkout paths. For
AWS, `environment/terraform-lab-inputs.json` contains the experiment-owned
Terraform variables; session ids, credentials, expiry, and provisioned endpoints
remain runtime bindings.

Project-owned Kubernetes resources are rendered by `render_project_manifests`
after provisioning supplies image digests and Kafka, Redis, and audit endpoints.
The resulting YAML is deterministic and can be archived as desired-state
evidence. Helm is represented only by explicitly versioned third-party releases;
custom application and load-test resources do not require a chart in this plan.

Run the same canonical source through the repository entrypoint:

```bash
demo/infra/run-experiment.sh \
  demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml \
  --environment internal-lab

demo/infra/run-experiment.sh \
  demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml \
  --environment aws
```

The shared command validates the selected environment before invoking an
adapter. Internal-lab runs use one bounded non-interactive root SSH command when
the caller is not root; AWS runs retain their disposable provisioning and
verified-cleanup lifecycle. `SIGINT` and `SIGTERM` are forwarded to the adapter
so its existing interruption and cleanup paths remain active during migration.
