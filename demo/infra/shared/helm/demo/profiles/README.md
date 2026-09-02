# Demo Helm Value Profiles

The internal-lab runner normally does not use static Helm profiles. It selects
a consumer profile from the shared `workloads/consumer-profiles.yaml` catalog,
computes a run plan, and writes a generated Helm values overlay under
`/opt/ckc-lab/state/generated`.

`demo.yaml` is kept as an internal-lab manual/debug overlay with common replica,
probe, and resource settings only. It intentionally does not define Kafka topic
partitions or consumer implementation settings.

AWS uses the same generated overlays: experiments select `target.profile`, and
the shared planner combines that consumer profile with the experiment-wide
`application` sizing and HPA contract. There are no AWS-specific compound
application profiles.
