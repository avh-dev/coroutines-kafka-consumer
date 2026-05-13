#!/usr/bin/env sh

set -eu

ENVIRONMENT="${1:-local}"
TEST_DEFINITION_PATH="${2:-demo/infra/shared/test-definitions/ckc-baseline-local.yaml}"
RUNNER_HOME="${3:-.ckc-runner/local-k8s}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
RUNNER_HOME_PATH="${REPO_ROOT}/${RUNNER_HOME}"

cd "${REPO_ROOT}"
python3 "${REPO_ROOT}/demo/infra/shared/test-orchestration/run-test.py" \
  --environment "${ENVIRONMENT}" \
  --region local \
  --repo-dir "${REPO_ROOT}" \
  --runner-home "${RUNNER_HOME_PATH}" \
  --test-definition-path "${TEST_DEFINITION_PATH}"
