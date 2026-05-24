#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/local-process-lib.sh"

PID_FILE="${REPO_ROOT}/.demo-infra/local-dev/pids/load-test.pid"

stop_process "load-test" "${PID_FILE}"
