#!/usr/bin/env bash

set +e

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
PROFILE_NAME="${3:-}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
TERRAFORM_DIR="${REPO_DIR}/infra/aws/assets/terraform/load-lab"
CLUSTER_NAME="ckc-load-lab-${ENVIRONMENT}"
KUBECONFIG_PATH="${CKC_RUNNER_KUBECONFIG_PATH:-${RUNNER_HOME}/kubeconfig/${CLUSTER_NAME}.yaml}"
LAB_CONTEXT_PATH="${RUNNER_HOME}/config/load-lab-${ENVIRONMENT}.json"

if [ -z "${PROFILE_NAME}" ] && [ -f "${LAB_CONTEXT_PATH}" ]; then
  PROFILE_NAME="$(python3 - <<PY
import json
from pathlib import Path

data = json.loads(Path("${LAB_CONTEXT_PATH}").read_text(encoding="utf-8"))
print(data.get("profile_name", ""))
PY
)"
fi

if [ -z "${PROFILE_NAME}" ]; then
  PROFILE_NAME="medium"
fi

PROFILE_PATH="${TERRAFORM_DIR}/profiles/${PROFILE_NAME}.tfvars"

mkdir -p "$(dirname "${KUBECONFIG_PATH}")"
aws eks update-kubeconfig --region "${REGION}" --name "${CLUSTER_NAME}" --kubeconfig "${KUBECONFIG_PATH}" 2>/dev/null
export KUBECONFIG="${KUBECONFIG_PATH}"

kubectl delete namespace ckc-loadtest --ignore-not-found=true
kubectl delete namespace ckc-app --ignore-not-found=true

if [ -f "${PROFILE_PATH}" ]; then
  terraform -chdir="${TERRAFORM_DIR}" init
  terraform -chdir="${TERRAFORM_DIR}" destroy -auto-approve -input=false \
    -var="aws_region=${REGION}" \
    -var="environment=${ENVIRONMENT}" \
    -var-file="${PROFILE_PATH}"
fi

rm -f "${LAB_CONTEXT_PATH}"
