# Demo Helm Value Profiles

The internal-lab runner normally does not use static Helm profiles. It selects
a consumer profile from the shared `workloads/consumer-profiles.yaml` catalog,
computes a run plan, and writes a generated Helm values overlay under
`/opt/ckc-lab/state/generated`.

`demo.yaml` is kept as an internal-lab manual/debug overlay with common replica,
probe, and resource settings only. It intentionally does not define Kafka topic
partitions or consumer implementation settings.

`aws/` contains legacy values for old single-definition AWS runs. Shared
experiments generate their application values from `target.profile` and do not
select these compound profiles.
