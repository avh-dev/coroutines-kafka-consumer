#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root." >&2
  exit 1
fi

echo "Removing internal lab Kubernetes workloads and k3s."
if command -v kubectl >/dev/null 2>&1 && [ -f /etc/rancher/k3s/k3s.yaml ]; then
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete namespace ckc-perf --ignore-not-found=true >/dev/null 2>&1 || true
fi

if [ -x /usr/local/bin/k3s-uninstall.sh ]; then
  /usr/local/bin/k3s-uninstall.sh || true
else
  systemctl stop k3s >/dev/null 2>&1 || true
  systemctl disable k3s >/dev/null 2>&1 || true
fi

echo "Removing internal lab Docker containers and images."
if command -v docker >/dev/null 2>&1; then
  if [ -f "${LAB_ROOT}/docker-compose.host-services.yml" ]; then
    LAB_NODE_IP="${LAB_NODE_IP:-127.0.0.1}" LAB_HOST="${LAB_HOST:-localhost}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" down --remove-orphans >/dev/null 2>&1 || true
  fi

  docker rm -f \
    ckc-perf-kafka \
    ckc-perf-redpanda \
    ckc-perf-redis \
    ckc-perf-demo-stubs \
    ckc-internal-grafana \
    ckc-internal-kafka-exporter \
    ckc-internal-cadvisor \
    ckc-internal-process-exporter >/dev/null 2>&1 || true

  docker image rm \
    ckc-perf/demo:latest \
    ckc-perf/demo-stubs:latest \
    docker.redpanda.com/redpandadata/redpanda:v25.1.3 \
    redis:7.4-alpine \
    grafana/grafana:11.6.0 \
    danielqsj/kafka-exporter:v1.8.0 \
    ncabatoff/process-exporter:0.8.7 >/dev/null 2>&1 || true
fi

echo "Removing lab files."
rm -rf "${LAB_ROOT}"
rm -rf /etc/rancher/k3s /var/lib/rancher/k3s /var/lib/kubelet /var/lib/cni /etc/cni /opt/cni
rm -f /usr/local/bin/helm
rm -f /etc/apt/sources.list.d/docker.list /etc/apt/keyrings/docker.asc

echo "Removing lab packages."
apt-get purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin openjdk-21-jre-headless >/dev/null 2>&1 || true
apt-get autoremove -y >/dev/null 2>&1 || true

echo "Internal lab server cleanup is complete."
