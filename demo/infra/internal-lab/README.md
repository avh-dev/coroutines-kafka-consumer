# Internal Lab

`demo/infra/internal-lab` runs the CKC demo on a dedicated Linux laptop with low overhead.

The app, demo stubs, Prometheus, and metrics-server run in k3s. Kafka, Redis, and Grafana run on the lab host through Docker Compose. Local state is stored under the repository root in `.demo-infra/internal-lab/`, which is ignored by Git.

## Requirements

- Lab host: Linux, preferably Ubuntu or Debian.
- Local machine: `root` SSH access to the lab host through a key.
- Local tools: Git Bash-compatible shell, `ssh`, `scp`, `kubectl`, `helm`, Docker, Java/Gradle.
- Windows users should run all local scripts from Git Bash, not PowerShell.

The lab host IP is provided to `install-lab.sh` and stored in `.demo-infra/internal-lab/lab.env`. Repository scripts and manifests should not hardcode a lab IP.

## Architecture

```text
local machine
  .demo-infra/internal-lab/kubeconfig.yaml -> k3s API on lab host
  kubectl / helm -> k3s
  Docker build -> image archive -> scp -> lab host
  load test -> app NodePort and Kafka on lab host

lab host
  k3s
    namespace ckc-perf
    ckc-demo Deployment, NodePort 30080
    ckc-demo-stubs Deployment, ClusterIP 8080
    ckc-prometheus Deployment, NodePort 30090
    metrics-server for kubectl top and HPA
    ckc-external-kafka Service + Endpoints -> host Kafka
    ckc-external-redis Service + Endpoints -> host Redis

  Docker Compose
    Kafka -> host:9092
    Redis -> host:6379
    Grafana -> host:3000
```

Prometheus stays in Kubernetes so it can use Kubernetes service discovery and scrape app pods plus kubelet cAdvisor metrics. Grafana stays on the host so it does not add pod overhead to the Kubernetes test surface.

## Install

From Git Bash at the repository root:

```sh
./demo/infra/internal-lab/scripts/install-lab.sh <LAB_IP>
```

The installer:

- writes `.demo-infra/internal-lab/lab.env`
- copies `demo/infra/internal-lab/assets` to `/opt/ckc-internal-lab/assets` on the lab host
- installs Docker, Helm, and k3s on the lab host
- starts k3s with `traefik`, `servicelb`, `local-storage`, and bundled `metrics-server` disabled
- deploys this lab's explicit metrics-server and Prometheus manifests
- starts host Kafka, Redis, and Grafana
- provisions Grafana datasource and the shared `CKC Overview` dashboard
- writes `.demo-infra/internal-lab/kubeconfig.yaml`
- verifies `kubectl`, Grafana, Prometheus, and Kafka from the local machine

After install:

```sh
source .demo-infra/internal-lab/lab.env
kubectl --kubeconfig "$KUBECONFIG" get nodes -o wide
curl -fsS "http://${LAB_HOST_IP}:3000/api/health"
curl -fsS "http://${LAB_HOST_IP}:30090/-/ready"
```

## Wake Lab Host

If the lab host supports Wake-on-LAN, wake it from the local machine with the
lab script:

```sh
./demo/infra/internal-lab/scripts/wakeup-lab.sh aa:bb:cc:dd:ee:ff --host 192.168.1.50 --wait-seconds 120
```

After install, you can store the host MAC next to the lab IP in local ignored
state:

```sh
echo 'LAB_HOST_MAC=aa:bb:cc:dd:ee:ff' >> .demo-infra/internal-lab/lab.env
./demo/infra/internal-lab/scripts/wakeup-lab.sh --wait-seconds 120
```

The wrapper delegates to a Python helper under `scripts/helpers/` that uses
only the Python standard library. It sends Wake-on-LAN magic packets to
`255.255.255.255:9` by default and can wait for SSH port `22` to become
reachable.

## Build Images

Build demo and stubs images locally, copy them to the lab, and load them into Docker and k3s containerd:

```sh
./demo/infra/internal-lab/scripts/build-load-images.sh
```

The app and stubs images are used by k3s.

## Prepare A Test

Select a test definition once:

```sh
./demo/infra/internal-lab/scripts/set-test.sh
```

The selection is saved under `.demo-infra/internal-lab/`.

Prepare the selected test definition:

```sh
./demo/infra/internal-lab/scripts/prepare-test.sh
```

You can still pass an explicit definition when needed:

```sh
./demo/infra/internal-lab/scripts/prepare-test.sh ckc-baseline-internal
```

This script:

- reads `deployment.app_profile`, `deployment.stubs_profile`, and `deployment.kafka_topics`
- removes any old demo HPA and scales the demo app down so consumer groups are inactive
- resets Redis on the lab host
- deletes stale Kafka consumer groups for the demo app
- deletes and recreates Kafka topics on the lab host
- deploys `ckc-demo-stubs` with the selected stubs Helm profile
- deploys `ckc-demo` with the selected app Helm profile

## Run A Test

Run the load generator locally as a Java process:

```sh
./demo/infra/internal-lab/scripts/run-test.sh
```

You can still pass an explicit definition when needed:

```sh
./demo/infra/internal-lab/scripts/run-test.sh ckc-baseline-internal
```

The script reads `load_test` settings from the test definition, exports them as environment variables for `ckc-demo-load-test`, and redirects stdout/stderr to:

```text
.demo-infra/internal-lab/logs/
```

It prints the Java process PID and the stop command:

```sh
kill <PID>
```

## Endpoints

Use `.demo-infra/internal-lab/lab.env` for the actual IP:

```text
App:        http://$LAB_HOST_IP:30080
Prometheus: http://$LAB_HOST_IP:30090
Grafana:    http://$LAB_HOST_IP:3000
Kafka:      $LAB_HOST_IP:9092
Redis:      $LAB_HOST_IP:6379
```

Grafana credentials are `admin` / `admin`.

The shared dashboard is provisioned under the `CKC` folder:

```text
http://$LAB_HOST_IP:3000/d/ckc-overview/ckc-overview
```

## Scaling And HPA

Manual scaling does not need metrics-server:

```sh
kubectl --kubeconfig "$KUBECONFIG" -n ckc-perf scale deployment ckc-demo --replicas=3
kubectl --kubeconfig "$KUBECONFIG" -n ckc-perf rollout status deployment/ckc-demo
```

`kubectl top` and HPA do need metrics-server:

```sh
kubectl --kubeconfig "$KUBECONFIG" top nodes
kubectl --kubeconfig "$KUBECONFIG" -n ckc-perf top pods
```

Enable the existing HPA profile when autoscaling itself is part of the test:

```sh
helm upgrade --install ckc-demo demo/infra/shared/helm/demo \
  --kubeconfig "$KUBECONFIG" \
  --namespace ckc-perf \
  -f demo/infra/internal-lab/assets/config/demo-values.yaml \
  -f demo/infra/shared/helm/demo/profiles/ckc-hpa.yaml

kubectl --kubeconfig "$KUBECONFIG" -n ckc-perf get hpa ckc-demo -w
```

For blocking vs non-blocking comparisons, prefer fixed manual replicas first. HPA changes pod count during the run and makes profile comparisons harder unless autoscaling behavior is the subject.

## Comparing Consumer Modes

Keep these constant between runs:

- Kafka topic partition counts
- app replicas
- app resource requests and limits
- stubs latency/error profile
- load profile
- warm-up and measurement window

Reset state and deploy the same test definition before every run:

```sh
./demo/infra/internal-lab/scripts/prepare-test.sh ckc-baseline-internal
```

While topics are being deleted and recreated, running app pods can briefly log `UNKNOWN_TOPIC_OR_PARTITION`. That should stop after `prepare-test` finishes and the topics exist again.

Switch only the app profile or explicit Helm settings. For example:

```sh
helm upgrade --install ckc-demo demo/infra/shared/helm/demo \
  --kubeconfig "$KUBECONFIG" \
  --namespace ckc-perf \
  -f demo/infra/internal-lab/assets/config/demo-values.yaml \
  -f demo/infra/shared/helm/demo/profiles/ckc-local-baseline.yaml \
  --set env.springProfilesActive=ckc
```

```sh
helm upgrade --install ckc-demo demo/infra/shared/helm/demo \
  --kubeconfig "$KUBECONFIG" \
  --namespace ckc-perf \
  -f demo/infra/internal-lab/assets/config/demo-values.yaml \
  -f demo/infra/shared/helm/demo/profiles/ckc-local-baseline.yaml \
  --set env.springProfilesActive=spring-kafka
```

Useful Prometheus queries:

```promql
sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="ckc-perf", container="demo"}[30s]))
sum by (pod) (container_memory_working_set_bytes{namespace="ckc-perf", container="demo"})
rate(ckc_poll_duration_seconds_count[30s])
sum by (consumer_impl, consumer_id) (rate(ckc_poll_records_sum[30s]))
sum(rate(ckc_poll_records_sum[30s])) by (consumer_impl, consumer_id)
sum(rate(ckc_processing_duration_seconds_count[30s])) by (consumer_impl, consumer_id)
sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"potion-tracking-.*"})
```

Use `kubectl top pods` for quick current snapshots. Use Prometheus/Grafana for profile comparisons because they preserve history and allow identical measurement windows.

## Verification

```sh
source .demo-infra/internal-lab/lab.env
kubectl --kubeconfig "$KUBECONFIG" -n ckc-perf get pods,svc,endpoints -o wide
kubectl --kubeconfig "$KUBECONFIG" -n ckc-perf top pods
curl -fsS "http://${LAB_HOST_IP}:30080/actuator/health"
curl -fsS "http://${LAB_HOST_IP}:30080/actuator/prometheus" | head
curl -fsS "http://${LAB_HOST_IP}:30090/api/v1/targets"
```

Host checks:

```sh
ssh "$SSH_TARGET" "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
ssh "$SSH_TARGET" "docker exec ckc-perf-redis redis-cli PING"
ssh "$SSH_TARGET" "docker exec ckc-perf-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${LAB_HOST_IP}:9092 --list"
```

## Benchmark Stability

Recommended before serious runs:

```sh
ssh "$SSH_TARGET" "LAB_ROOT=${LAB_ROOT} ${LAB_ROOT}/assets/scripts/tune-host.sh"
./demo/infra/internal-lab/scripts/prepare-test.sh ckc-baseline-internal
```

Use wired Ethernet when possible. Keep load generation off the lab host. Run a warm-up before measuring. Keep Prometheus scrape interval at `5s` or slower unless short spike visibility is required.

The default demo app values set memory limits but do not set CPU limits. CPU limits can introduce CFS throttling and distort latency measurements. Add CPU limits only when testing constrained CPU behavior.

## Typical Pitfalls

- Kafka `advertised.listeners` points at `localhost`.
- The app uses `localhost` for Redis, Kafka, or stubs from inside Kubernetes.
- Images were rebuilt locally but not loaded into k3s.
- Kafka topics were not recreated between tests.
- CPU limits cause throttling and look like application latency.
- The load generator runs on the lab host and competes with the app.
- Prometheus scrape interval is too aggressive.
- Swap is enabled or CPU governor is `powersave`.
- Windows scripts are run from PowerShell instead of Git Bash.
