#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
LOCAL_PORT="${2:-9093}"
INSTANCE_ID="${3:-}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TERRAFORM_DIR="${REPO_DIR}/infra/aws/terraform/runner"

if [ -z "${INSTANCE_ID}" ]; then
  INSTANCE_ID="$(terraform -chdir="${TERRAFORM_DIR}" output -raw instance_id)"
fi

aws ssm start-session \
  --target "${INSTANCE_ID}" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=9090,localPortNumber=${LOCAL_PORT}" \
  --region "${REGION}"
