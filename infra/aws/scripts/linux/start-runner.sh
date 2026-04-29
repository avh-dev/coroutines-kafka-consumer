#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
INSTANCE_ID="${2:-}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TERRAFORM_DIR="${REPO_DIR}/infra/aws/terraform/runner"

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

if [ -z "${INSTANCE_ID}" ]; then
  INSTANCE_ID="$(terraform -chdir="${TERRAFORM_DIR}" output -raw instance_id)"
fi

aws ec2 start-instances --instance-ids "${INSTANCE_ID}" --region "${REGION}"
aws ec2 wait instance-running --instance-ids "${INSTANCE_ID}" --region "${REGION}"
aws ec2 wait instance-status-ok --instance-ids "${INSTANCE_ID}" --region "${REGION}"
wait_for_ssm_online "${INSTANCE_ID}"
wait_for_runner_bootstrap "${INSTANCE_ID}"
printf 'Runner instance is running: %s\n' "${INSTANCE_ID}"
