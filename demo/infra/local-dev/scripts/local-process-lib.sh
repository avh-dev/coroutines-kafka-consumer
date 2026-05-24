#!/usr/bin/env bash

set -euo pipefail

ensure_dir() {
  mkdir -p "$1"
}

is_pid_running() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

read_pid_file() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    tr -d '[:space:]' < "${pid_file}"
  fi
}

require_not_running() {
  local name="$1"
  local pid_file="$2"
  local pid=""

  pid="$(read_pid_file "${pid_file}")"
  if is_pid_running "${pid}"; then
    echo "${name} is already running with pid=${pid}." >&2
    exit 1
  fi

  rm -f "${pid_file}"
}

stop_process() {
  local name="$1"
  local pid_file="$2"
  local timeout_seconds="${3:-20}"
  local pid=""

  pid="$(read_pid_file "${pid_file}")"
  if [[ -z "${pid}" ]]; then
    echo "${name} is not running; pid file was not found."
    return
  fi

  if ! is_pid_running "${pid}"; then
    echo "${name} is not running; removing stale pid file."
    rm -f "${pid_file}"
    return
  fi

  kill "${pid}" >/dev/null 2>&1 || true
  for _ in $(seq 1 "${timeout_seconds}"); do
    if ! is_pid_running "${pid}"; then
      rm -f "${pid_file}"
      echo "${name} stopped."
      return
    fi
    sleep 1
  done

  echo "${name} did not stop after ${timeout_seconds}s; sending SIGKILL."
  kill -9 "${pid}" >/dev/null 2>&1 || true
  rm -f "${pid_file}"
}

resolve_config_file() {
  local config_dir="$1"
  local config_name="${2:-}"

  if [[ -n "${config_name}" ]]; then
    if [[ "${config_name}" == */* || "${config_name}" == *\\* ]]; then
      [[ "${config_name}" == *.env ]] || config_name="${config_name}.env"
      printf '%s\n' "${config_name}"
      return
    fi

    [[ "${config_name}" == *.env ]] || config_name="${config_name}.env"
    printf '%s\n' "${config_dir}/${config_name}"
    return
  fi

  mapfile -t configs < <(find "${config_dir}" -maxdepth 1 -type f -name '*.env' -printf '%f\n' | sort)
  if [[ "${#configs[@]}" -eq 0 ]]; then
    echo "No env files found in ${config_dir}." >&2
    exit 1
  fi

  if [[ ! -t 0 ]]; then
    echo "Config name is required in non-interactive mode. Available configs:" >&2
    printf '  %s\n' "${configs[@]%.env}" >&2
    exit 1
  fi

  echo "Select config:" >&2
  select selected in "${configs[@]%.env}"; do
    if [[ -n "${selected}" ]]; then
      printf '%s\n' "${config_dir}/${selected}.env"
      return
    fi
    echo "Invalid selection." >&2
  done
}

source_env_file() {
  local env_file="$1"

  if [[ ! -f "${env_file}" ]]; then
    echo "Env file was not found: ${env_file}" >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
}
