#!/usr/bin/env bash

set -uo pipefail

runtime_dir="${KAFKA_RUNTIME_DIR:-/var/run/ckc-warmup-kafka}"
ready_file="${KAFKA_READY_FILE:-${runtime_dir}/ready}"
shutdown_timeout="${KAFKA_SHUTDOWN_TIMEOUT_SECONDS:-30}"
bootstrap="${KAFKA_BOOTSTRAP_COMMAND:-/opt/ckc-warmup-kafka/bin/bootstrap-topics.sh}"
lifecycle_topic="${KAFKA_BROKER_LIFECYCLE_TOPIC:-}"
lifecycle_cli="${KAFKA_BROKER_LIFECYCLE_COMMAND:-/opt/ckc-warmup-kafka/bin/broker-lifecycle.sh}"
lifecycle_address="${KAFKA_BOOTSTRAP_SERVER:-localhost:19092}"
drain_timeout="${KAFKA_DRAIN_TIMEOUT_SECONDS:-50}"
termination_budget="${KAFKA_TERMINATION_GRACE_PERIOD_SECONDS:-90}"
startup_timeout="${KAFKA_OPERATION_TIMEOUT_SECONDS:-10}"
stopping=false
broker_pid=""
bootstrap_pid=""
witness_pid=""
epoch=""
epoch_file="${runtime_dir}/broker-epoch"
topics_ready_file="${ready_file}"

if ! [[ "${shutdown_timeout}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KAFKA_SHUTDOWN_TIMEOUT_SECONDS must be a positive integer: ${shutdown_timeout}" >&2
  exit 2
fi
if [[ "$#" -eq 0 ]]; then
  echo "Kafka broker command is required." >&2
  exit 2
fi
if [[ -n "${lifecycle_topic}" ]]; then
  for setting in "${drain_timeout}" "${shutdown_timeout}" "${termination_budget}" "${startup_timeout}"; do
    if ! [[ "${setting}" =~ ^[1-9][0-9]{0,5}$ ]]; then
      echo "Warm-up Kafka lifecycle budgets must be positive integers of at most six digits." >&2
      exit 2
    fi
  done
  if (( drain_timeout < 3 || drain_timeout + shutdown_timeout + 5 > termination_budget )); then
    echo "Warm-up Kafka grace period must include drain, Kafka shutdown, and a five-second margin." >&2
    exit 2
  fi
  epoch="$(cat /proc/sys/kernel/random/uuid)" || exit 2
  topics_ready_file="${runtime_dir}/topics-ready"
  if [[ -n "${CKC_WARMUP_POD_IP:-}" ]]; then
    host="${CKC_WARMUP_POD_IP}"
    if [[ "${host}" == *:* ]]; then host="[${host}]"; fi
    export KAFKA_ADVERTISED_LISTENERS="INTERNAL://localhost:19092,EXTERNAL://${host}:9092"
  fi
fi

mkdir -p "${runtime_dir}"
rm -f -- "${ready_file}" "${topics_ready_file}" "${epoch_file}" "${runtime_dir}/prewarm.json" \
  "${runtime_dir}/broker.pid" "${runtime_dir}/bootstrap.pid"

terminate_children() {
  local reason="${1:-exit}" deadline pid drain_pid drain_status
  [[ "${stopping}" == "false" ]] || return
  stopping=true
  rm -f -- "${ready_file}"
  if [[ -n "${bootstrap_pid}" ]]; then
    kill -TERM "${bootstrap_pid}" 2>/dev/null || true
  fi
  if [[ "${reason}" == signal && -n "${lifecycle_topic}" ]]; then
    if [[ -s "${epoch_file}" && "$(< "${epoch_file}")" == "${epoch}" ]] && kill -0 "${broker_pid}" 2>/dev/null; then
      timeout --signal=KILL "${drain_timeout}" "${lifecycle_cli}" drain \
        "${lifecycle_address}" "${lifecycle_topic}" "${epoch}" "$(((drain_timeout - 2) * 1000))" &
      drain_pid=$!
      while :; do
        drain_status=0
        wait "${drain_pid}" || drain_status=$?
        # Another signal may interrupt wait. The first drain command still owns the deadline.
        if ! kill -0 "${drain_pid}" 2>/dev/null; then break; fi
      done
      if (( drain_status > 2 )); then
        printf 'warmup-kafka-supervisor status=CONTROL_UNAVAILABLE brokerEpoch=%s cliExit=%s\n' \
          "${epoch}" "${drain_status}" >&2
      fi
    else
      printf 'warmup-kafka-supervisor status=CONTROL_UNAVAILABLE brokerEpoch=%s reason=broker_not_ready\n' \
        "${epoch}" >&2
    fi
  fi
  if [[ -n "${broker_pid}" ]] && kill -0 "${broker_pid}" 2>/dev/null; then
    printf '%s warmup-kafka-supervisor event=KafkaSIGTERM brokerEpoch=%s pid=%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${epoch}" "${broker_pid}" >&2
    kill -TERM "${broker_pid}" 2>/dev/null || true
  fi
  deadline=$((SECONDS + shutdown_timeout))
  while (( SECONDS < deadline )); do
    if { [[ -z "${bootstrap_pid}" ]] || ! kill -0 "${bootstrap_pid}" 2>/dev/null; } &&
      ! kill -0 "${broker_pid}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  if [[ -n "${witness_pid}" ]]; then
    kill -TERM "${witness_pid}" 2>/dev/null || true
  fi
  for pid in "${bootstrap_pid}" "${broker_pid}" "${witness_pid}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  done
}

trap 'terminate_children signal' TERM INT HUP

"$@" &
broker_pid=$!
printf '%s\n' "${broker_pid}" > "${runtime_dir}/broker.pid"
KAFKA_READY_FILE="${topics_ready_file}" "${bootstrap}" &
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
    if [[ -n "${lifecycle_topic}" ]]; then
      "${lifecycle_cli}" serve "${lifecycle_address}" "${lifecycle_topic}" \
        "${epoch}" "$((startup_timeout * 1000))" "${epoch_file}" &
      witness_pid=$!
    fi
  fi
  if [[ -n "${witness_pid}" ]]; then
    if ! kill -0 "${witness_pid}" 2>/dev/null; then
      wait "${witness_pid}" || exit_status=$?
      failed_process=lifecycle
      exit_status=$((exit_status == 0 ? 1 : exit_status))
      break
    fi
    if [[ "${stopping}" == false && ! -s "${ready_file}" && -s "${epoch_file}" &&
      "$(< "${epoch_file}")" == "${epoch}" ]]; then
      printf '%s\n' "$(date +%s)" > "${ready_file}"
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
if [[ -n "${witness_pid}" ]]; then
  wait "${witness_pid}" 2>/dev/null || true
fi
wait "${broker_pid}" 2>/dev/null || true
if [[ -n "${epoch}" ]]; then
  printf '%s warmup-kafka-supervisor event=KafkaStopped brokerEpoch=%s pid=%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${epoch}" "${broker_pid}" >&2
fi
rm -f -- "${runtime_dir}/broker.pid" "${runtime_dir}/bootstrap.pid" "${ready_file}" \
  "${topics_ready_file}" "${epoch_file}"
exit "${exit_status}"
