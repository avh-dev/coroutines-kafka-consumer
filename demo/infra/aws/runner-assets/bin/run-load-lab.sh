#!/usr/bin/env bash

set -euo pipefail

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
TEST_DEFINITION_PATH="${3:-}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-/opt/ckc-runner/assets/repo}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"

COMMAND=(
  "${REPO_DIR}/demo/infra/aws/runner-assets/bin/run-test.sh"
  "${REGION}"
  "${ENVIRONMENT}"
)

if [ -n "${TEST_DEFINITION_PATH}" ]; then
  COMMAND+=("${TEST_DEFINITION_PATH}")
fi

exec "${COMMAND[@]}"
