#!/usr/bin/env bash

set -uo pipefail

runtime_dir="${KAFKA_RUNTIME_DIR:-/var/run/ckc-warmup-kafka}"
ready_file="${KAFKA_READY_FILE:-${runtime_dir}/ready}"
shutdown_timeout="${KAFKA_SHUTDOWN_TIMEOUT_SECONDS:-30}"
reconciler="${KAFKA_RECONCILER_COMMAND:-/opt/ckc-warmup-kafka/bin/reconcile-topics.sh}"
stopping=false
broker_pid=""
reconciler_pid=""

if ! [[ "${shutdown_timeout}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KAFKA_SHUTDOWN_TIMEOUT_SECONDS must be a positive integer: ${shutdown_timeout}" >&2
  exit 2
fi
if [[ "$#" -eq 0 ]]; then
  echo "Kafka broker command is required." >&2
  exit 2
fi

mkdir -p "${runtime_dir}"
rm -f -- "${ready_file}" "${runtime_dir}/broker.pid" "${runtime_dir}/reconciler.pid"

terminate_children() {
  local deadline pid
  [[ "${stopping}" == "false" ]] || return
  stopping=true
  rm -f -- "${ready_file}"
  for pid in "${reconciler_pid}" "${broker_pid}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  deadline=$((SECONDS + shutdown_timeout))
  while (( SECONDS < deadline )); do
    if ! kill -0 "${reconciler_pid}" 2>/dev/null && ! kill -0 "${broker_pid}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  for pid in "${reconciler_pid}" "${broker_pid}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  done
}

trap 'terminate_children' TERM INT HUP

"$@" &
broker_pid=$!
printf '%s\n' "${broker_pid}" > "${runtime_dir}/broker.pid"
"${reconciler}" &
reconciler_pid=$!
printf '%s\n' "${reconciler_pid}" > "${runtime_dir}/reconciler.pid"

exit_status=0
failed_process=""
while :; do
  if ! kill -0 "${broker_pid}" 2>/dev/null; then
    wait "${broker_pid}" || exit_status=$?
    failed_process=broker
    break
  fi
  if ! kill -0 "${reconciler_pid}" 2>/dev/null; then
    wait "${reconciler_pid}" || exit_status=$?
    if [[ "${exit_status}" -eq 0 && "${stopping}" == "false" ]]; then
      exit_status=1
    fi
    failed_process=reconciler
    break
  fi
  sleep 0.2
done

if [[ "${stopping}" == "false" ]]; then
  echo "warmup-kafka-supervisor process=${failed_process} exit_status=${exit_status}" >&2
fi
terminate_children
wait "${reconciler_pid}" 2>/dev/null || true
wait "${broker_pid}" 2>/dev/null || true
rm -f -- "${runtime_dir}/broker.pid" "${runtime_dir}/reconciler.pid" "${ready_file}"
exit "${exit_status}"
