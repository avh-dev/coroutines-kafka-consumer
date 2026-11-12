# MVP lab infrastructure

This is the opt-in environment contract for GLOBAL-4 experiments. The broker remains an
independent optional transport even though the application now uses the library warm-up API.

Place these settings in a canonical experiment definition:

```yaml
environments:
  internal-lab:
    lab:
      profile: installed
    warmup_kafka:
      enabled: true
      placement: application
      image: apache/kafka:4.3.1
      heap: 512Mi
      resources:
        requests: {cpu: 500m, memory: 768Mi}
        limits: {cpu: '1', memory: 1Gi}
      topics:
        - {name: ckc.traffic-warmup.v1.control, partitions: 1}
        - {name: ckc.traffic-warmup.v1.order.events, partitions: 1}
        - {name: ckc.traffic-warmup.v1.batch.events, partitions: 1}
        - {name: ckc.traffic-warmup.v1.cauldron.events, partitions: 1}
```

Use this placement on each target:

```yaml
placement:
  application: worker
  stubs: worker
  generator: controller
```

In this lab `optilab` is the controller/support host, and `optilab2` is the application
worker. The existing placement adapter maps those roles to actual node selectors.
Keep the same infrastructure and placement on cold and warm-up targets. The broker and
its explicitly listed topics are experiment environment inputs, not inferred from old
demo role flags. Source topics remain on the primary Kafka cluster. Topic names must
use `ckc.traffic-warmup.v1.*`, separate from broker warm-up's `ckc.warmup.v1.*` namespace.

Kafka uses ephemeral storage and one replica. A sidecar provisions topics before pod
readiness. The Service publishes not-ready endpoints so Kafka's advertised address is
reachable by the provisioner. The runner waits up to ten minutes for that preparation
before applying the application manifest; preparation failure aborts the experiment.
This lab preparation gate is separate from the library's eventual cold-start fallback.

After application shutdown, cleanup deletes the broker Deployment and Service, then
verifies that neither resource nor its pods remain. API errors fail cleanup explicitly.

The renderer supplies warm-up bootstrap and security-protocol environment variables to
the demo integration. Main Kafka connection properties remain independent. The
broker image is pulled by Kubernetes with `IfNotPresent`; this optional service does not
add a required image to every lab image rebuild. The worker needs registry access or an
operator-preloaded matching image. Custom pinned images must provide the Apache image's
entrypoint and `/opt/kafka/bin` tools.

These resource values reproduce the earlier lab configuration and are not production
sizing. AWS deployment is deliberately rejected until its adapter and resource placement
are qualified. The internal-lab evidence bundle captures the broker's actual node, image, heap,
requests, limits, topics, CPU, memory, and network utilization so later AWS sizing starts
from measured evidence rather than the request alone.

The canonical A/B definition is
`demo/infra/experiments/ckc-simplified-traffic-warmup-scale-up-16k-local.yaml`. Both targets
provision the same broker and application resources; the only runtime difference is
`TRAFFIC_WARMUP_ENABLED`. Four application-side packet captures filter the worker-local
broker port during the ramp. Their decoded Produce requests provide records-per-request and
record-batch evidence for the small 10-record-per-second warm-up budget. Prometheus evidence
adds delegation outcomes, fallback and timeout counts, pending-result peak, send and result
latencies, and process-start-to-main-subscription time.

No experiment is launched by provisioning tests. After installing the definition, an
operator can explicitly request the A/B run; ordinary repository validation only resolves
and materializes it.
