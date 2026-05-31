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
EOF
  fi
}

write_default_configs
ensure_dir "${LOG_DIR}"
ensure_dir "${PID_DIR}"
require_not_running "demo-stubs" "${PID_FILE}"

ENV_FILE="$(resolve_config_file "${CONFIG_DIR}" "${1:-baseline}")"
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
