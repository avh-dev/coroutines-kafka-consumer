#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
INSTANCE_ID="${2:-}"
ARTIFACT_BUCKET="${3:-}"
ARTIFACT_KEY="${4:-runner-assets/runner-assets.tar.gz}"
RUNNER_ASSETS_DIR="${CKC_RUNNER_ASSETS_DIR:-/opt/ckc-runner/assets}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LOCAL_REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../../.." && pwd)"
TERRAFORM_DIR="${LOCAL_REPO_DIR}/demo/infra/aws/terraform/runner"
TEMP_DIR="${LOCAL_REPO_DIR}/.demo-infra/tmp"
BUNDLE_TARGET="${RUNNER_ASSETS_DIR}/runner-assets.tar.gz"
REPO_TARGET="${RUNNER_ASSETS_DIR}/repo"

if [ -z "${INSTANCE_ID}" ]; then
  INSTANCE_ID="$(terraform -chdir="${TERRAFORM_DIR}" output -raw instance_id)"
fi

mkdir -p "${TEMP_DIR}"
BUNDLE_FILE="$(mktemp "${TEMP_DIR}/ckc-runner-assets.XXXXXX.tar.gz")"
COMMANDS_FILE="$(mktemp "${TEMP_DIR}/ckc-runner-update.XXXXXX.json")"

cleanup() {
  rm -f "${BUNDLE_FILE}" "${COMMANDS_FILE}"
}

trap cleanup EXIT

cd "${LOCAL_REPO_DIR}"
tar -czf "${BUNDLE_FILE}" \
  demo/infra/aws/assets/terraform/load-lab/main.tf \
  demo/infra/aws/assets/terraform/load-lab/variables.tf \
  demo/infra/aws/assets/terraform/load-lab/versions.tf \
  demo/infra/aws/assets/terraform/load-lab/outputs.tf \
  demo/infra/aws/assets/terraform/load-lab/profiles \
  demo/infra/aws/assets/terraform/load-lab/terraform.tfvars.example \
  demo/infra/aws/audit \
  demo/infra/aws/experiments \
  demo/infra/aws/runner-assets \
  demo/infra/aws/restore \
  demo/infra/aws/test-definitions \
  demo/infra/internal-lab/assets/helpers \
  demo/infra/internal-lab/assets/restore \
  demo/infra/shared/audit \
  demo/infra/shared/experiment_orchestration \
  demo/infra/shared/experiment_report \
  demo/infra/shared/pcap \
  demo/infra/shared/grafana \
  demo/infra/shared/helm \
  demo/infra/shared/result_bundle \
  demo/infra/shared/test-orchestration \
  demo/infra/shared/workloads

if [ -n "${ARTIFACT_BUCKET}" ]; then
  aws s3 cp "${BUNDLE_FILE}" "s3://${ARTIFACT_BUCKET}/${ARTIFACT_KEY}" --region "${REGION}" --only-show-errors
  cat > "${COMMANDS_FILE}" <<EOF
{
  "commands": [
    "set -euo pipefail",
    "command -v tshark >/dev/null 2>&1 || dnf install -y wireshark-cli",
    "mkdir -p \"${RUNNER_ASSETS_DIR}\"",
    "aws s3 cp \"s3://${ARTIFACT_BUCKET}/${ARTIFACT_KEY}\" \"${BUNDLE_TARGET}\" --region \"${REGION}\" --only-show-errors",
    "mkdir -p \"${REPO_TARGET}\"",
    "tar -xzf \"${BUNDLE_TARGET}\" -C \"${REPO_TARGET}\"",
    "find \"${REPO_TARGET}/demo/infra/aws/runner-assets/bin\" -type f -name '*.sh' -exec chmod +x {} +",
    "find \"${REPO_TARGET}/demo/infra/aws/restore\" -type f -name '*.sh' -exec chmod +x {} +",
    "find \"${REPO_TARGET}/demo/infra/aws/audit\" -type f -name '*.sh' -exec chmod +x {} +",
    "mkdir -p /opt/ckc-runner/observability/grafana/provisioning/dashboards /opt/ckc-runner/observability/grafana/provisioning/datasources /opt/ckc-runner/observability/grafana/dashboards",
    "cp \"${REPO_TARGET}/demo/infra/shared/grafana/provisioning/dashboards/ckc.yml\" /opt/ckc-runner/observability/grafana/provisioning/dashboards/ckc.yml",
    "cp \"${REPO_TARGET}/demo/infra/shared/grafana/provisioning/datasources/prometheus.yml\" /opt/ckc-runner/observability/grafana/provisioning/datasources/prometheus.yml",
    "cp \"${REPO_TARGET}/demo/infra/shared/result_bundle/restore/loki-datasource.yml\" /opt/ckc-runner/observability/grafana/provisioning/datasources/loki.yml",
    "cp \"${REPO_TARGET}/demo/infra/shared/grafana/dashboards/ckc-overview.json\" /opt/ckc-runner/observability/grafana/dashboards/ckc-overview.json",
    "echo synced=true",
    "echo repo_dir=${REPO_TARGET}"
  ]
}
EOF
else
  BUNDLE_BASE64="$(base64 < "${BUNDLE_FILE}" | tr -d '\n')"
  cat > "${COMMANDS_FILE}" <<EOF
{
  "commands": [
    "set -euo pipefail",
    "command -v tshark >/dev/null 2>&1 || dnf install -y wireshark-cli",
    "mkdir -p \\"${RUNNER_ASSETS_DIR}\\"",
    "python3 - <<'PY'",
    "import base64, pathlib",
    "data = \\"\\"\\\"${BUNDLE_BASE64}\\"\\"\\\"",
    "pathlib.Path(\\"${BUNDLE_TARGET}\\").write_bytes(base64.b64decode(data))",
    "PY",
    "mkdir -p \\"${REPO_TARGET}\\"",
    "tar -xzf \\"${BUNDLE_TARGET}\\" -C \\"${REPO_TARGET}\\"",
    "find \\"${REPO_TARGET}/demo/infra/aws/runner-assets/bin\\" -type f -name '*.sh' -exec chmod +x {} +",
    "find \\"${REPO_TARGET}/demo/infra/aws/restore\\" -type f -name '*.sh' -exec chmod +x {} +",
    "find \\"${REPO_TARGET}/demo/infra/aws/audit\\" -type f -name '*.sh' -exec chmod +x {} +",
    "mkdir -p /opt/ckc-runner/observability/grafana/provisioning/dashboards /opt/ckc-runner/observability/grafana/provisioning/datasources /opt/ckc-runner/observability/grafana/dashboards",
    "cp \\"${REPO_TARGET}/demo/infra/shared/grafana/provisioning/dashboards/ckc.yml\\" /opt/ckc-runner/observability/grafana/provisioning/dashboards/ckc.yml",
    "cp \\"${REPO_TARGET}/demo/infra/shared/grafana/provisioning/datasources/prometheus.yml\\" /opt/ckc-runner/observability/grafana/provisioning/datasources/prometheus.yml",
    "cp \\"${REPO_TARGET}/demo/infra/shared/result_bundle/restore/loki-datasource.yml\\" /opt/ckc-runner/observability/grafana/provisioning/datasources/loki.yml",
    "cp \\"${REPO_TARGET}/demo/infra/shared/grafana/dashboards/ckc-overview.json\\" /opt/ckc-runner/observability/grafana/dashboards/ckc-overview.json",
    "echo synced=true",
    "echo repo_dir=${REPO_TARGET}"
  ]
}
EOF
fi

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
