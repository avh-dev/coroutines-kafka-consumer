#!/usr/bin/env bash

set +e

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
TERRAFORM_DIR="${REPO_DIR}/demo/infra/aws/assets/terraform/load-lab"
CLUSTER_NAME="ckc-load-lab-${ENVIRONMENT}"
KUBECONFIG_PATH="${CKC_RUNNER_KUBECONFIG_PATH:-${RUNNER_HOME}/kubeconfig/${CLUSTER_NAME}.yaml}"
LAB_CONTEXT_PATH="${RUNNER_HOME}/config/load-lab-${ENVIRONMENT}.json"
SKIP_TERRAFORM="${CKC_LOAD_LAB_SKIP_TERRAFORM:-false}"

if [ -f "${LAB_CONTEXT_PATH}" ]; then
  CONTEXT_VALUES="$(python3 - <<PY
import json
from pathlib import Path

data = json.loads(Path("${LAB_CONTEXT_PATH}").read_text(encoding="utf-8"))
print(data.get("cluster_name", ""))
PY
)"
  CONTEXT_CLUSTER_NAME="$(printf '%s\n' "${CONTEXT_VALUES}" | sed -n '1p')"
  if [ -n "${CONTEXT_CLUSTER_NAME}" ]; then
    CLUSTER_NAME="${CONTEXT_CLUSTER_NAME}"
    KUBECONFIG_PATH="${CKC_RUNNER_KUBECONFIG_PATH:-${RUNNER_HOME}/kubeconfig/${CLUSTER_NAME}.yaml}"
  fi
fi

mkdir -p "$(dirname "${KUBECONFIG_PATH}")"
aws eks update-kubeconfig --region "${REGION}" --name "${CLUSTER_NAME}" --kubeconfig "${KUBECONFIG_PATH}" 2>/dev/null
export KUBECONFIG="${KUBECONFIG_PATH}"

kubectl delete namespace ckc-loadtest --ignore-not-found=true
kubectl delete namespace ckc-app --ignore-not-found=true
kubectl delete namespace ckc-observability --ignore-not-found=true
docker rm -f ckc-msk-cloudwatch-exporter ckc-msk-cloudwatch-vmagent >/dev/null 2>&1

if [ "${SKIP_TERRAFORM}" != "true" ]; then
  terraform -chdir="${TERRAFORM_DIR}" init
  terraform -chdir="${TERRAFORM_DIR}" destroy -auto-approve -input=false \
    -var="aws_region=${REGION}" \
    -var="environment=${ENVIRONMENT}"
fi

rm -f "${LAB_CONTEXT_PATH}"
