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
| Order / batch producer linger | 500 ms | 300 ms |
| Telemetry producer linger | 150 ms | 300 ms |
| Order / batch producer batch.size | 32 KiB | 512 KiB |
| Telemetry producer batch.size | 16 KiB | 128 KiB |
| Order / batch consumer fetch.min.bytes | 8 KiB | 64 KiB |
| Telemetry consumer fetch.min.bytes | 8 KiB | 16 KiB |
| Order / batch consumer fetch.max.wait.ms | 600 ms | 350 ms |
| Telemetry consumer fetch.max.wait.ms | 250 ms | 350 ms |
| Consumer max.poll.records | 500 | 2000 |
| Processing workers per topic | Partition listeners: 58 / 41 / 182 | 500 / 500 / 500 |

All targets use one producer per topic (capacity setting 5000 TPS), LZ4,
64 MiB buffer.memory per producer, 50 MiB fetch.max.bytes, and 1 MiB
max.partition.fetch.bytes. The old capacity setting of 1000 TPS created two
independent producers per topic, splitting accumulator batches further.
Increasing max.poll.records does not itself increase fetch sizes.

At peak traffic, an even-key approximation gives Spring about 15 order records
and 15 batch records per partition over 500 ms. At the planned mean handling
time of 25 ms, draining such a burst takes about 375 ms. Telemetry produces about
1.65 records per partition over 150 ms; at 70 ms each, draining that burst takes
about 116 ms. These are planning estimates, not latency bounds: key skew, handler
tails and queues can consume the remaining margin. A one-second linger on Spring
would leave substantially less margin.

CKC has two partitions per topic: before batch-size limits, one producer can
accumulate approximately 263 order, 188 batch, and 300 telemetry records per
partition over the shared 300 ms linger interval. The prior capture at
`20260914T115237Z` contains about 332 uncompressed bytes per record averaged
across all topics. Using 512 bytes per record as a provisional sizing allowance
gives about 131 / 94 / 150 KiB at the new linger. The existing 512 KiB order and
batch limits leave ample headroom; the 128 KiB telemetry limit remains above the
rough 97 KiB estimate from the measured average record size. Neither estimate is
a measured maximum or per-topic percentile. Inspect actual batch sizes and
compression after the run and revise the limits if necessary.

The measured candidates showed why producer and consumer waits must be aligned.
A 50 ms fetch wait caused 36,600–46,000 Spring Fetch requests per 15 seconds.
Raising it only to 250 ms still produced 20,911–21,591 requests while producers
used about 15,000 record batches. Consumer traffic remained roughly twice the
producer traffic. A pending Fetch normally overlaps producer accumulation, so
its maximum wait is not an additional delay after linger. Spring fetch waits
remain 100 ms above their corresponding producer linger; CKC uses a 350 ms fetch
wait for every topic, 50 ms above its shared 300 ms producer linger.

The latest comparison still showed higher Spring consumer traffic than producer
traffic: 281 pollers formed only about 4.7 records per batch, while CKC used
three poll loops and about 470 records per batch. A later tuning iteration should
first increase Spring producer `batch.size`, then measure whether fewer partitions
and pollers preserve latency while reducing consumer wire traffic.

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
