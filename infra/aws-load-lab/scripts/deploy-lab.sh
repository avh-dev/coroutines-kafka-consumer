#!/usr/bin/env sh

set -eu

REGION="${1:-eu-central-1}"
ENVIRONMENT="${2:-dev}"
CLUSTER_NAME="ckc-load-lab-${ENVIRONMENT}"

aws eks update-kubeconfig --region "${REGION}" --name "${CLUSTER_NAME}"

kubectl create namespace ckc-app --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace ckc-loadtest --dry-run=client -o yaml | kubectl apply -f -

printf 'Namespaces are ready.\n'
printf 'Next steps:\n'
printf '  1. Install Kafka and Redis into namespace ckc-app.\n'
printf '  2. Deploy demo-stubs, demo app, Prometheus, and Grafana.\n'
printf '  3. Run load-test jobs in namespace ckc-loadtest.\n'
