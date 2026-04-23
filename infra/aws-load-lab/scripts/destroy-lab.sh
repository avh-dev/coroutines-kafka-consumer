#!/usr/bin/env sh

set +e

REGION="${1:-eu-central-1}"
ENVIRONMENT="${2:-dev}"

kubectl delete namespace ckc-loadtest --ignore-not-found=true
kubectl delete namespace ckc-app --ignore-not-found=true

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "${SCRIPT_DIR}/../terraform" || exit 1

terraform destroy -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"
