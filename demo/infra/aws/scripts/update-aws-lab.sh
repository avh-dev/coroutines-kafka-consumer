#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
INSTANCE_ID="${3:-}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TERRAFORM_DIR="${REPO_DIR}/demo/infra/aws/terraform/runner"

usage() {
  cat <<EOF
Usage: $0 [region] [environment] [runner-instance-id]

Builds local demo images, pushes them to ECR, and syncs runner-side AWS lab
assets to the long-lived runner. Run long-lived lab and test commands from the
runner itself, preferably inside tmux.
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

if [ -z "${INSTANCE_ID}" ]; then
  INSTANCE_ID="$(terraform -chdir="${TERRAFORM_DIR}" output -raw instance_id)"
fi

echo "Building and pushing AWS lab images."
"${SCRIPT_DIR}/libexec/build-and-push.sh" "${REGION}" "${ENVIRONMENT}"

echo "Syncing runner assets."
"${SCRIPT_DIR}/libexec/sync-runner-assets.sh" "${REGION}" "${INSTANCE_ID}"

echo "AWS lab assets are updated."
echo "  runner_instance=${INSTANCE_ID}"
