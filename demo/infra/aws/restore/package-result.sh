#!/usr/bin/env bash

set -euo pipefail

RESULT_DIR="${1:?Usage: $0 result-directory}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
TARGET_DIR="${RESULT_DIR}/restore"

python3 "${SCRIPT_DIR}/finalize-result.py" "${RESULT_DIR}" --repo-root "${REPO_ROOT}"

mkdir -p "${TARGET_DIR}/grafana"
cp "${SCRIPT_DIR}/open-result.sh" "${TARGET_DIR}/open-result.sh"
cp "${SCRIPT_DIR}/close-result.sh" "${TARGET_DIR}/close-result.sh"
cp "${SCRIPT_DIR}/docker-compose.yml" "${TARGET_DIR}/docker-compose.yml"
cp "${SCRIPT_DIR}/README.md" "${TARGET_DIR}/README.md"
cp "${SCRIPT_DIR}/finalize-result.py" "${TARGET_DIR}/finalize-result.py"
cp "${REPO_ROOT}/demo/infra/internal-lab/assets/restore/import-loki.py" "${TARGET_DIR}/import-loki.py"
cp "${REPO_ROOT}/demo/infra/shared/result_bundle/restore/import-grafana-annotations.py" "${TARGET_DIR}/import-grafana-annotations.py"
cp "${REPO_ROOT}/demo/infra/shared/result_bundle/restore/loki.yaml" "${TARGET_DIR}/loki.yaml"
cp -R "${REPO_ROOT}/demo/infra/shared/grafana/provisioning" "${TARGET_DIR}/grafana/"
cp "${REPO_ROOT}/demo/infra/shared/result_bundle/restore/loki-datasource.yml" "${TARGET_DIR}/grafana/provisioning/datasources/loki.yml"
chmod 0755 "${TARGET_DIR}/open-result.sh" "${TARGET_DIR}/close-result.sh" "${TARGET_DIR}/import-grafana-annotations.py"

echo "Restore kit packaged at ${TARGET_DIR}"
