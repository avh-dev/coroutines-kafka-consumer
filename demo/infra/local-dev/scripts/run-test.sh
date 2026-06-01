#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"
STATE_DIR="${REPO_ROOT}/.demo-infra/local-dev"
PID_DIR="${STATE_DIR}/pids"
LOAD_TEST_PID_FILE="${PID_DIR}/load-test.pid"
STUBS_PID_FILE="${PID_DIR}/stubs.pid"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/local-process-lib.sh"

compose_services_running() {
  local expected=("kafka" "redis" "prometheus" "grafana")
  local service=""

  for service in "${expected[@]}"; do
    if ! docker compose -f "${COMPOSE_FILE}" ps --services --status running | grep -Fxq "${service}"; then
      return 1
    fi
  done

  if ! docker inspect -f '{{.State.Running}}' ckc-local-redpanda 2>/dev/null | grep -Fxq "true"; then
    return 1
  fi

  return 0
}

wait_for_container_health() {
  local container="$1"
  local deadline_seconds="${2:-120}"
  local deadline=$((SECONDS + deadline_seconds))
  local status=""

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
      return
    fi
    sleep 2
  done

  echo "${container} did not become ready in ${deadline_seconds}s." >&2
  docker logs "${container}" --tail 80 >&2 || true
  exit 1
}

wait_for_http() {
  local url="$1"
  local deadline_seconds="${2:-60}"
  local deadline=$((SECONDS + deadline_seconds))

  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done

  echo "${url} did not become ready in ${deadline_seconds}s." >&2
  exit 1
}

ensure_compose() {
  if compose_services_running; then
    echo "Local docker compose is already running."
    return
  fi

  echo "Local docker compose is not fully running. Recreating local-dev containers."
  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans
  docker compose -f "${COMPOSE_FILE}" up -d --remove-orphans
  wait_for_container_health "ckc-local-redpanda"
  wait_for_container_health "ckc-local-redis"
}

read_pid() {
  read_pid_file "$1"
}

show_run_summary() {
  local stubs_config="$1"
  local test_config="$2"
  local load_pid="$3"
  local stubs_pid="$4"
  local stubs_log="$5"
  local test_log="$6"

  echo
  echo "Local test is running."
  echo "  compose=${COMPOSE_FILE}"
  echo "  stubs_config=${stubs_config}"
  echo "  stubs_pid=${stubs_pid}"
  echo "  stubs_log=${stubs_log}"
  echo "  test_config=${test_config}"
  echo "  load_test_pid=${load_pid}"
  echo "  load_test_log=${test_log}"
  echo
  if [[ -t 0 ]]; then
    echo "Press q to stop the test early. Otherwise this script exits when the load-test process finishes."
  else
    echo "No interactive input is attached; waiting until the load-test process finishes."
  fi
}

stop_requested() {
  local key=""

  if [[ ! -t 0 ]]; then
    return 1
  fi

  if IFS= read -r -s -n 1 -t 1 key < /dev/tty; then
    [[ "${key}" == "q" || "${key}" == "Q" ]]
    return
  fi

  return 1
}

stop_started_processes() {
  stop_process "load-test" "${LOAD_TEST_PID_FILE}" 10
  stop_process "demo-stubs" "${STUBS_PID_FILE}" 10
}

main() {
  local stubs_config=""
  local test_config=""
  local stubs_log=""
  local test_log=""
  local load_pid=""
  local stubs_pid=""

  ensure_dir "${PID_DIR}"
  ensure_compose

  echo
  echo "Starting demo stubs."
  "${SCRIPT_DIR}/stubs.sh"
  stubs_pid="$(read_pid "${STUBS_PID_FILE}")"
  stubs_config="$(read_pid "${PID_DIR}/stubs.env")"
  stubs_log="$(ls -t "${STATE_DIR}/logs"/stubs-*.log 2>/dev/null | head -n 1 || true)"
  source_env_file "${stubs_config}"
  wait_for_http "http://127.0.0.1:${PORT:-18080}/health"
  trap stop_started_processes EXIT INT TERM

  echo
  echo "Resetting local Redis."
  docker exec ckc-local-redis redis-cli FLUSHALL

  echo
  echo "Recreating local topics."
  "${SCRIPT_DIR}/create-topics.sh"

  echo
  echo "Starting load test."
  "${SCRIPT_DIR}/test.sh"
  load_pid="$(read_pid "${LOAD_TEST_PID_FILE}")"
  test_config="$(read_pid "${PID_DIR}/load-test.env")"
  test_log="$(ls -t "${STATE_DIR}/logs"/load-test-*.log 2>/dev/null | head -n 1 || true)"

  show_run_summary "${stubs_config}" "${test_config}" "${load_pid}" "${stubs_pid}" "${stubs_log}" "${test_log}"

  while true; do
    if ! is_pid_running "${load_pid}"; then
      rm -f "${LOAD_TEST_PID_FILE}"
      echo "Load test finished."
      break
    fi

    if stop_requested; then
      echo "Stopping load test by user request."
      stop_process "load-test" "${LOAD_TEST_PID_FILE}" 10
      break
    fi
  done

  stop_process "demo-stubs" "${STUBS_PID_FILE}" 10
  trap - EXIT INT TERM
}

main "$@"
