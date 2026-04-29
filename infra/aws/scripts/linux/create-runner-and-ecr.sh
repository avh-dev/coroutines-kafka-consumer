#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
RUNNER_TERRAFORM_DIR="${REPO_DIR}/infra/aws/terraform/runner"
ECR_TERRAFORM_DIR="${REPO_DIR}/infra/aws/terraform/ecr"

wait_for_ssm_online() {
  local instance_id="$1"
  local status=""
  local attempts=0
  while [ "${attempts}" -lt 60 ]; do
    status="$(aws ssm describe-instance-information \
      --region "${REGION}" \
      --filters "Key=InstanceIds,Values=${instance_id}" \
      --query "InstanceInformationList[0].PingStatus" \
      --output text 2>/dev/null || true)"
    if [ "${status}" = "Online" ]; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 10
  done
  printf 'Runner instance did not become SSM-online in time: %s\n' "${instance_id}" >&2
  return 1
}

wait_for_runner_bootstrap() {
  local instance_id="$1"
  local command_id=""
  command_id="$(aws ssm send-command \
    --region "${REGION}" \
    --instance-ids "${instance_id}" \
    --document-name "AWS-RunShellScript" \
    --comment "Wait for CKC runner bootstrap" \
    --parameters commands='["cloud-init status --wait","systemctl is-active ckc-runner-observability.service","docker ps --format {{.Names}} | grep -q prometheus","docker ps --format {{.Names}} | grep -q grafana"]' \
    --query "Command.CommandId" \
    --output text)"
  aws ssm wait command-executed --region "${REGION}" --command-id "${command_id}" --instance-id "${instance_id}"
}

sync_runner_assets() {
  "${SCRIPT_DIR}/update-runner.sh" "${REGION}" "$1"
}

if [ ! -f "${RUNNER_TERRAFORM_DIR}/terraform.tfvars" ]; then
  cp "${RUNNER_TERRAFORM_DIR}/terraform.tfvars.example" "${RUNNER_TERRAFORM_DIR}/terraform.tfvars"
fi

if [ ! -f "${ECR_TERRAFORM_DIR}/terraform.tfvars" ]; then
  cp "${ECR_TERRAFORM_DIR}/terraform.tfvars.example" "${ECR_TERRAFORM_DIR}/terraform.tfvars"
fi

terraform -chdir="${RUNNER_TERRAFORM_DIR}" init
terraform -chdir="${RUNNER_TERRAFORM_DIR}" apply -auto-approve -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"

INSTANCE_ID="$(terraform -chdir="${RUNNER_TERRAFORM_DIR}" output -raw instance_id)"
aws ec2 wait instance-status-ok --region "${REGION}" --instance-ids "${INSTANCE_ID}"
wait_for_ssm_online "${INSTANCE_ID}"
wait_for_runner_bootstrap "${INSTANCE_ID}"
sync_runner_assets "${INSTANCE_ID}"

terraform -chdir="${ECR_TERRAFORM_DIR}" init
terraform -chdir="${ECR_TERRAFORM_DIR}" apply -auto-approve -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"

terraform -chdir="${RUNNER_TERRAFORM_DIR}" output
terraform -chdir="${ECR_TERRAFORM_DIR}" output
