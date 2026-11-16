#!/usr/bin/env bash

set -uo pipefail

runtime_dir="${KAFKA_RUNTIME_DIR:-/var/run/ckc-warmup-kafka}"
ready_file="${KAFKA_READY_FILE:-${runtime_dir}/ready}"
shutdown_timeout="${KAFKA_SHUTDOWN_TIMEOUT_SECONDS:-30}"
bootstrap="${KAFKA_BOOTSTRAP_COMMAND:-/opt/ckc-warmup-kafka/bin/bootstrap-topics.sh}"
stopping=false
broker_pid=""
bootstrap_pid=""

if ! [[ "${shutdown_timeout}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KAFKA_SHUTDOWN_TIMEOUT_SECONDS must be a positive integer: ${shutdown_timeout}" >&2
  exit 2
fi
if [[ "$#" -eq 0 ]]; then
  echo "Kafka broker command is required." >&2
  exit 2
fi

mkdir -p "${runtime_dir}"
rm -f -- "${ready_file}" "${runtime_dir}/broker.pid" "${runtime_dir}/bootstrap.pid"

terminate_children() {
  local deadline pid
  [[ "${stopping}" == "false" ]] || return
  stopping=true
  rm -f -- "${ready_file}"
  for pid in "${bootstrap_pid}" "${broker_pid}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  deadline=$((SECONDS + shutdown_timeout))
  while (( SECONDS < deadline )); do
    if { [[ -z "${bootstrap_pid}" ]] || ! kill -0 "${bootstrap_pid}" 2>/dev/null; } &&
      ! kill -0 "${broker_pid}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  for pid in "${bootstrap_pid}" "${broker_pid}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  done
}

trap 'terminate_children' TERM INT HUP

"$@" &
broker_pid=$!
printf '%s\n' "${broker_pid}" > "${runtime_dir}/broker.pid"
"${bootstrap}" &
bootstrap_pid=$!
printf '%s\n' "${bootstrap_pid}" > "${runtime_dir}/bootstrap.pid"

exit_status=0
failed_process=""
while :; do
  if ! kill -0 "${broker_pid}" 2>/dev/null; then
    wait "${broker_pid}" || exit_status=$?
    failed_process=broker
    break
  fi
  if [[ -n "${bootstrap_pid}" ]] && ! kill -0 "${bootstrap_pid}" 2>/dev/null; then
    wait "${bootstrap_pid}" || exit_status=$?
    rm -f -- "${runtime_dir}/bootstrap.pid"
    bootstrap_pid=""
    if [[ "${exit_status}" -ne 0 ]]; then
      failed_process=bootstrap
      break
    fi
  fi
  sleep 0.2
done

if [[ "${stopping}" == "false" ]]; then
  echo "warmup-kafka-supervisor process=${failed_process} exit_status=${exit_status}" >&2
fi
terminate_children
if [[ -n "${bootstrap_pid}" ]]; then
  wait "${bootstrap_pid}" 2>/dev/null || true
fi
wait "${broker_pid}" 2>/dev/null || true
rm -f -- "${runtime_dir}/broker.pid" "${runtime_dir}/bootstrap.pid" "${ready_file}"
exit "${exit_status}"
