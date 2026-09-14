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

## No-chaos E2E tuning candidate

`spring-ckc-no-chaos-e2e-tuning-5k.yaml` keeps the 5k/s workload, 4–9 minute
measurement window, five-second telemetry cadence, and packet capture, with no
chaos. It is an initial candidate for tuning each implementation; measured
results must establish whether it meets the limits before calling it optimized.
The first target is also tuned. Comparisons include different producer settings
and should be described as complete configurations, not isolated client effects.

| Setting | Spring JDK / Armeria | CKC |
| --- | --- | --- |
| Order / batch producer linger | 700 ms | 1000 ms |
| Telemetry producer linger | 250 ms | 250 ms |
| Order / batch producer batch.size | 32 KiB | 512 KiB |
| Telemetry producer batch.size | 16 KiB | 128 KiB |
| Order / batch consumer fetch.min.bytes | 8 KiB | 64 KiB |
| Telemetry consumer fetch.min.bytes | 1 KiB | 16 KiB |
| Consumer fetch.max.wait.ms | 50 ms | 50 ms |
| Consumer max.poll.records | 500 | 2000 |
| Processing workers per topic | Partition listeners: 58 / 41 / 182 | 300 / 300 / 300 |

All targets use one producer per topic (capacity setting 5000 TPS), LZ4,
64 MiB buffer.memory per producer, 50 MiB fetch.max.bytes, and 1 MiB
max.partition.fetch.bytes. The old capacity setting of 1000 TPS created two
independent producers per topic, splitting accumulator batches further.
Increasing max.poll.records does not itself increase fetch sizes.

At peak traffic, an even-key approximation gives Spring about 21 order records
and 21 batch records per partition over 700 ms. At the planned mean handling
time of 25 ms, draining such a burst takes about 525 ms. Linger plus fetch wait
plus draining is about 1275 ms, leaving roughly 725 ms against the 2 s limit.
Telemetry produces about 2.75 records per partition over 250 ms; at 70 ms each,
the equivalent estimate is about 493 ms against the 1 s limit. These are planning
estimates, not latency bounds: key skew, handler tails and queues can consume
the margin. A one-second linger on Spring would leave substantially less margin.

CKC has two partitions per topic: before batch-size limits, one producer can
accumulate approximately 875 order, 625 batch, and 250 telemetry records per
partition over their respective linger intervals. The prior capture at
`20260914T115237Z` contains about 332 uncompressed bytes per record averaged
across all topics. Using 512 bytes per record as a provisional sizing allowance
gives about 438 / 313 / 125 KiB, motivating 512 / 512 / 128 KiB batch limits.
This allowance is not a measured maximum or per-topic percentile. Inspect actual
batch sizes and compression after the run and revise it if necessary.

The audit calculates E2E from Kafka record timestamp to completion; with
CreateTime timestamps this includes producer linger. Keep timestamp semantics
consistent and verify the recorded configuration. The tuning objective is zero
observed exceedances at 2 s for orders/batches and 1 s for processed telemetry.
The current canonical schema carries these as topic reference limits, not an
automatic acceptance gate (evaluation may be NOT_EVALUATED). Inspect the report
against these limits. Also inspect published-cohort on-time outcomes and telemetry drops:
dropping old records must not masquerade as a latency improvement. Check the
full run and the measurement window separately; a configured limit is not a PASS.

For the next iteration, compare actual batch occupancy, compression, producer
buffer pressure, consumer lag, E2E p95/p99/max and the on-time fraction. Increase
linger only with measured tail-latency headroom; reduce it or fetch wait when
queues consume the budget. More CKC workers help only until downstream or key
serialization becomes the bottleneck.

Kafka parameter semantics: [producer configuration](https://kafka.apache.org/41/configuration/producer-configs/)
and [consumer configuration](https://kafka.apache.org/41/configuration/consumer-configs/).
