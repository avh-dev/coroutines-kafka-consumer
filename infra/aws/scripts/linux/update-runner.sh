#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
INSTANCE_ID="${2:-}"
RUNNER_ASSETS_DIR="${CKC_RUNNER_ASSETS_DIR:-/opt/ckc-runner/assets}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LOCAL_REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TERRAFORM_DIR="${LOCAL_REPO_DIR}/infra/aws/terraform/runner"
BUNDLE_TARGET="${RUNNER_ASSETS_DIR}/runner-assets.tar.gz"
REPO_TARGET="${RUNNER_ASSETS_DIR}/repo"

if [ -z "${INSTANCE_ID}" ]; then
  INSTANCE_ID="$(terraform -chdir="${TERRAFORM_DIR}" output -raw instance_id)"
fi

BUNDLE_FILE="$(mktemp "${TMPDIR:-/tmp}/ckc-runner-assets.XXXXXX.tar.gz")"
COMMANDS_FILE="$(mktemp "${TMPDIR:-/tmp}/ckc-runner-update.XXXXXX.json")"

cleanup() {
  rm -f "${BUNDLE_FILE}" "${COMMANDS_FILE}"
}

trap cleanup EXIT

cd "${LOCAL_REPO_DIR}"
tar -czf "${BUNDLE_FILE}" \
  infra/aws/assets/terraform/load-lab/main.tf \
  infra/aws/assets/terraform/load-lab/variables.tf \
  infra/aws/assets/terraform/load-lab/versions.tf \
  infra/aws/assets/terraform/load-lab/outputs.tf \
  infra/aws/assets/terraform/load-lab/profiles \
  infra/aws/assets/terraform/load-lab/terraform.tfvars.example \
  infra/aws/runner-internal \
  infra/shared/grafana \
  infra/shared/helm \
  infra/shared/test-definitions \
  infra/shared/test-orchestration

BUNDLE_BASE64="$(base64 < "${BUNDLE_FILE}" | tr -d '\n')"

cat > "${COMMANDS_FILE}" <<EOF
{
  "commands": [
    "set -euo pipefail",
    "mkdir -p \\"${RUNNER_ASSETS_DIR}\\"",
    "python3 - <<'PY'",
    "import base64, pathlib",
    "data = \\"\\"\\\"${BUNDLE_BASE64}\\"\\"\\\"",
    "pathlib.Path(\\"${BUNDLE_TARGET}\\").write_bytes(base64.b64decode(data))",
    "PY",
    "mkdir -p \\"${REPO_TARGET}\\"",
    "tar -xzf \\"${BUNDLE_TARGET}\\" -C \\"${REPO_TARGET}\\"",
    "find \\"${REPO_TARGET}/infra/aws/runner-internal\\" -type f -name '*.sh' -exec chmod +x {} +",
    "mkdir -p /opt/ckc-runner/observability/grafana/provisioning/dashboards /opt/ckc-runner/observability/grafana/provisioning/datasources /opt/ckc-runner/observability/grafana/dashboards",
    "cp \\"${REPO_TARGET}/infra/shared/grafana/provisioning/dashboards/ckc.yml\\" /opt/ckc-runner/observability/grafana/provisioning/dashboards/ckc.yml",
    "cp \\"${REPO_TARGET}/infra/shared/grafana/provisioning/datasources/prometheus.yml\\" /opt/ckc-runner/observability/grafana/provisioning/datasources/prometheus.yml",
    "cp \\"${REPO_TARGET}/infra/shared/grafana/dashboards/ckc-overview.json\\" /opt/ckc-runner/observability/grafana/dashboards/ckc-overview.json",
    "echo synced=true",
    "echo repo_dir=${REPO_TARGET}"
  ]
}
EOF

COMMAND_ID="$(aws ssm send-command \
  --region "${REGION}" \
  --instance-ids "${INSTANCE_ID}" \
  --document-name "AWS-RunShellScript" \
  --comment "Sync CKC runner assets" \
  --parameters "file://${COMMANDS_FILE}" \
  --query "Command.CommandId" \
  --output text)"

aws ssm wait command-executed --region "${REGION}" --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}"
aws ssm get-command-invocation --region "${REGION}" --command-id "${COMMAND_ID}" --instance-id "${INSTANCE_ID}" --query "StandardOutputContent" --output text
