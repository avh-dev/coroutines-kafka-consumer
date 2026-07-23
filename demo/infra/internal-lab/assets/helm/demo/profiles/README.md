# Internal-Lab Demo Helm Profiles

The internal-lab runner normally does not use static Helm profiles. It selects
a consumer profile from the shared `workloads/consumer-profiles.yaml` catalog,
computes a run plan, and writes a generated Helm values overlay under
`/opt/ckc-lab/state/generated`.

`demo.yaml` is kept as a generic manual/debug overlay with common replica,
probe, and resource settings only. It intentionally does not define Kafka topic
partitions or consumer implementation settings.
