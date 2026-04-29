#!/usr/bin/env bash

set -euo pipefail

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
TEST_DEFINITION_PATH="${3:-}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"

COMMAND=(
  python3
  "${REPO_DIR}/infra/aws/assets/runner/run-test.py"
  --region "${REGION}"
  --environment "${ENVIRONMENT}"
  --repo-dir "${REPO_DIR}"
  --runner-home "${RUNNER_HOME}"
)

if [ -n "${TEST_DEFINITION_PATH}" ]; then
  COMMAND+=(--test-definition-path "${TEST_DEFINITION_PATH}")
fi

exec "${COMMAND[@]}"
