#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
LOAD_LAB_TERRAFORM_DIR="${REPO_DIR}/infra/aws/assets/terraform/load-lab"
RUNNER_TERRAFORM_DIR="${REPO_DIR}/infra/aws/terraform/runner"
ECR_TERRAFORM_DIR="${REPO_DIR}/infra/aws/terraform/ecr"

kubectl delete namespace ckc-loadtest --ignore-not-found=true || true
kubectl delete namespace ckc-app --ignore-not-found=true || true

terraform -chdir="${LOAD_LAB_TERRAFORM_DIR}" init -input=false
terraform -chdir="${LOAD_LAB_TERRAFORM_DIR}" destroy -auto-approve -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"

terraform -chdir="${RUNNER_TERRAFORM_DIR}" init -input=false
terraform -chdir="${ECR_TERRAFORM_DIR}" init -input=false

terraform -chdir="${ECR_TERRAFORM_DIR}" destroy -auto-approve -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"
terraform -chdir="${RUNNER_TERRAFORM_DIR}" destroy -auto-approve -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"
