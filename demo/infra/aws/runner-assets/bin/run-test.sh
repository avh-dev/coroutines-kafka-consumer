#!/usr/bin/env bash

set -euo pipefail

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
TEST_DEFINITION_PATH="${3:-}"
RUN_ID="${4:-}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"

COMMAND=(
  python3
  "${REPO_DIR}/demo/infra/shared/test-orchestration/run-test.py"
  --region "${REGION}"
  --environment "${ENVIRONMENT}"
  --repo-dir "${REPO_DIR}"
  --runner-home "${RUNNER_HOME}"
)

if [ -n "${TEST_DEFINITION_PATH}" ]; then
  COMMAND+=(--test-definition-path "${TEST_DEFINITION_PATH}")
fi
if [ -n "${RUN_ID}" ]; then
  COMMAND+=(--run-id "${RUN_ID}")
fi

exec "${COMMAND[@]}"
