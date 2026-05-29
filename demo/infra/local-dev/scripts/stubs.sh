#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/local-process-lib.sh"

STATE_DIR="${REPO_ROOT}/.demo-infra/local-dev"
CONFIG_DIR="${STATE_DIR}/stubs-env"
LOG_DIR="${STATE_DIR}/logs"
PID_DIR="${STATE_DIR}/pids"
PID_FILE="${PID_DIR}/stubs.pid"

write_default_configs() {
  ensure_dir "${CONFIG_DIR}"

  if [[ ! -f "${CONFIG_DIR}/baseline.env" ]]; then
    cat > "${CONFIG_DIR}/baseline.env" <<'EOF'
PORT=18080
STUB_WORKERS=64
ETA_DELAY_P90_MS=10
ETA_DELAY_P95_MS=50
ETA_DELAY_P99_MS=150
ETA_DELAY_P100_MS=300
FLAVOUR_DELAY_P90_MS=10
FLAVOUR_DELAY_P95_MS=50
FLAVOUR_DELAY_P99_MS=150
FLAVOUR_DELAY_P100_MS=300
ERROR_RATE_PERCENT=0
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/fast.env" ]]; then
    cat > "${CONFIG_DIR}/fast.env" <<'EOF'
PORT=18080
STUB_WORKERS=64
ETA_DELAY_P90_MS=2
ETA_DELAY_P95_MS=5
ETA_DELAY_P99_MS=10
ETA_DELAY_P100_MS=20
FLAVOUR_DELAY_P90_MS=2
FLAVOUR_DELAY_P95_MS=5
FLAVOUR_DELAY_P99_MS=10
FLAVOUR_DELAY_P100_MS=20
ERROR_RATE_PERCENT=0
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/slow.env" ]]; then
    cat > "${CONFIG_DIR}/slow.env" <<'EOF'
PORT=18080
STUB_WORKERS=64
ETA_DELAY_P90_MS=100
ETA_DELAY_P95_MS=250
ETA_DELAY_P99_MS=750
ETA_DELAY_P100_MS=1500
FLAVOUR_DELAY_P90_MS=100
FLAVOUR_DELAY_P95_MS=250
FLAVOUR_DELAY_P99_MS=750
FLAVOUR_DELAY_P100_MS=1500
ERROR_RATE_PERCENT=0
EOF
  fi
}

write_default_configs
ensure_dir "${LOG_DIR}"
ensure_dir "${PID_DIR}"
require_not_running "demo-stubs" "${PID_FILE}"

ENV_FILE="$(resolve_config_file "${CONFIG_DIR}" "${1:-}")"
source_env_file "${ENV_FILE}"
printf '%s\n' "${ENV_FILE}" > "${PID_DIR}/stubs.env"

cd "${REPO_ROOT}"
./gradlew :ckc-demo-stubs:installDist

STUBS_BIN="${REPO_ROOT}/demo/ckc-demo-stubs/build/install/ckc-demo-stubs/bin/ckc-demo-stubs"
if [[ ! -f "${STUBS_BIN}" ]]; then
  echo "Demo stubs runtime was not found: ${STUBS_BIN}" >&2
  exit 1
fi

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_FILE="${LOG_DIR}/stubs-${RUN_ID}.log"
nohup bash "${STUBS_BIN}" > "${LOG_FILE}" 2>&1 &
PID="$!"
echo "${PID}" > "${PID_FILE}"

echo "Demo stubs started."
echo "  config=${ENV_FILE}"
echo "  pid=${PID}"
echo "  log=${LOG_FILE}"
echo "  url=http://localhost:${PORT:-8080}"
