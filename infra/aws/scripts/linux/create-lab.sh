#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
PROFILE_NAME="${3:-default}"
DEFAULT_TEST_DEFINITION_PATH="infra/shared/test-definitions/ckc-baseline.yaml"
ARG4="${4:-}"
if [ -n "${ARG4}" ] && [ "${ARG4#i-}" != "${ARG4}" ]; then
  TEST_DEFINITION_PATH="${CKC_TEST_DEFINITION_PATH:-${DEFAULT_TEST_DEFINITION_PATH}}"
  INSTANCE_ID="${ARG4}"
else
  TEST_DEFINITION_PATH="${CKC_TEST_DEFINITION_PATH:-${ARG4:-${DEFAULT_TEST_DEFINITION_PATH}}}"
  INSTANCE_ID="${5:-}"
fi
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
    "cd \\"\\\${RUNNER_REPO_DIR}\\"",
    "CKC_RUNNER_REPO_DIR=\\"\\\${RUNNER_REPO_DIR}\\" ./infra/aws/runner-internal/create-lab.sh ${REGION} ${ENVIRONMENT} ${PROFILE_NAME} ${TEST_DEFINITION_PATH}"
  ]
}
EOF

COMMAND_ID="$(aws ssm send-command \
  --region "${REGION}" \
  --instance-ids "${INSTANCE_ID}" \
  --document-name "AWS-RunShellScript" \
  --comment "Create CKC load lab" \
  --parameters "file://${COMMANDS_FILE}" \
  --query "Command.CommandId" \
  --output text)"

aws ssm wait command-executed --region "${REGION}" --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}"
aws ssm get-command-invocation --region "${REGION}" --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}" --query "StandardOutputContent" --output text

rm -f "${COMMANDS_FILE}"
