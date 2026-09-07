# Experiments

This is the only author-maintained experiment catalog. Every YAML file contains
its workload, topic traffic and latency limits, explicit targets, and supported
environment definitions. It must not refer to another test, SLA, consumer, Helm,
or Terraform profile.

Validate an experiment before provisioning:

```bash
python3 demo/infra/shared/validate-experiment.py \
  demo/infra/experiments/smoke-repeat.yaml \
  --environment internal-lab
```

Run it from any repository checkout through the shared entrypoint:

```bash
demo/infra/run-experiment.sh demo/infra/experiments/smoke-repeat.yaml \
  --environment internal-lab

demo/infra/run-experiment.sh demo/infra/experiments/smoke.yaml \
  --environment aws
```

An experiment may contain both environment entries when its workload is
portable. Environment-specific capabilities are checked before any deployment.
Generated planner capabilities, Terraform inputs, Kubernetes manifests, and
resolved snapshots are evidence, not additional configuration sources.
