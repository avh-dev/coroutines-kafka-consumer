#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
TEST_DEFINITION_PATH="${3:-infra/shared/test-definitions/ckc-baseline.yaml}"
INSTANCE_ID="${4:-}"
REPO_DIR_ON_RUNNER="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LOCAL_REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TERRAFORM_DIR="${LOCAL_REPO_DIR}/infra/aws/terraform/runner"

if [ -z "${INSTANCE_ID}" ]; then
  INSTANCE_ID="$(terraform -chdir="${TERRAFORM_DIR}" output -raw instance_id)"
fi

"${SCRIPT_DIR}/update-runner.sh" "${REGION}" "${INSTANCE_ID}" >/dev/null

COMMANDS_FILE="$(mktemp)"
cat > "${COMMANDS_FILE}" <<EOF
{
  "commands": [
    "set -euo pipefail",
    "RUNNER_REPO_DIR=${REPO_DIR_ON_RUNNER}",
    "TEST_DEFINITION_PATH=${TEST_DEFINITION_PATH}",
    "if [ ! -f \\"\\\${RUNNER_REPO_DIR}/infra/aws/runner-internal/run-test.sh\\" ]; then echo Runner assets are expected at \\\${RUNNER_REPO_DIR} >&2; exit 1; fi",
    "mkdir -p /opt/ckc-runner/reports",
    "LOG_FILE=/opt/ckc-runner/reports/test-launch-\\$(date -u +%Y%m%dT%H%M%SZ).log",
    "cd \\"\\\${RUNNER_REPO_DIR}\\"",
    "CKC_RUNNER_REPO_DIR=\\"\\\${RUNNER_REPO_DIR}\\" nohup ./infra/aws/runner-internal/run-test.sh ${REGION} ${ENVIRONMENT} \\"\\\${TEST_DEFINITION_PATH}\\" > \\"\\\${LOG_FILE}\\" 2>&1 < /dev/null &",
    "echo started=true",
    "echo log_file=\\\${LOG_FILE}"
  ]
}
EOF

COMMAND_ID="$(aws ssm send-command \
  --region "${REGION}" \
  --instance-ids "${INSTANCE_ID}" \
  --document-name "AWS-RunShellScript" \
  --comment "Launch CKC test run" \
  --parameters "file://${COMMANDS_FILE}" \
  --query "Command.CommandId" \
  --output text)"

aws ssm wait command-executed --region "${REGION}" --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}"
aws ssm get-command-invocation --region "${REGION}" --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}" --query "StandardOutputContent" --output text

rm -f "${COMMANDS_FILE}"
