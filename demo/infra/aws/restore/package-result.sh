#!/usr/bin/env bash

set -euo pipefail

RESULT_DIR="${1:?Usage: $0 result-directory}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TARGET_DIR="${RESULT_DIR}/restore"

mkdir -p "${TARGET_DIR}/grafana"
cp "${SCRIPT_DIR}/open-result.sh" "${TARGET_DIR}/open-result.sh"
cp "${SCRIPT_DIR}/close-result.sh" "${TARGET_DIR}/close-result.sh"
cp "${SCRIPT_DIR}/docker-compose.yml" "${TARGET_DIR}/docker-compose.yml"
cp "${SCRIPT_DIR}/README.md" "${TARGET_DIR}/README.md"
cp -R "${REPO_ROOT}/demo/infra/shared/grafana/provisioning" "${TARGET_DIR}/grafana/"
chmod 0755 "${TARGET_DIR}/open-result.sh" "${TARGET_DIR}/close-result.sh"

echo "Restore kit packaged at ${TARGET_DIR}"
