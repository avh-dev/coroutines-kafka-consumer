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
