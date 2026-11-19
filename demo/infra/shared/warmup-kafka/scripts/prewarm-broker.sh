#!/usr/bin/env bash

set -uo pipefail

KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:19092}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-/var/run/ckc-warmup-kafka/client.properties}"
KAFKA_PREWARM_EVIDENCE_FILE="${KAFKA_PREWARM_EVIDENCE_FILE:-/var/run/ckc-warmup-kafka/prewarm.json}"
KAFKA_PREWARM_TIMEOUT_SECONDS="${KAFKA_PREWARM_TIMEOUT_SECONDS:-180}"
KAFKA_PREWARM_KILL_AFTER_SECONDS="${KAFKA_PREWARM_KILL_AFTER_SECONDS:-5}"
KAFKA_PREWARM_JAVA_COMMAND="${KAFKA_PREWARM_JAVA_COMMAND:-/opt/java/openjdk/bin/java}"
child_pid=""

for setting in KAFKA_PREWARM_TIMEOUT_SECONDS KAFKA_PREWARM_KILL_AFTER_SECONDS; do
  value="${!setting}"
  if ! [[ "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${setting} must be a positive integer: ${value}" >&2
    exit 2
  fi
done

terminate_child() {
  if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" 2>/dev/null; then
    kill -TERM "${child_pid}" 2>/dev/null || true
  fi
}
trap terminate_child TERM INT HUP

mkdir -p "$(dirname -- "${KAFKA_PREWARM_EVIDENCE_FILE}")"
rm -f -- "${KAFKA_PREWARM_EVIDENCE_FILE}"

timeout --signal=TERM --kill-after="${KAFKA_PREWARM_KILL_AFTER_SECONDS}" \
  "${KAFKA_PREWARM_TIMEOUT_SECONDS}" \
  "${KAFKA_PREWARM_JAVA_COMMAND}" -cp "/opt/ckc-warmup-kafka/classes:/opt/kafka/libs/*" \
  ckc.warmup.kafka.PrewarmBroker \
  "${KAFKA_BOOTSTRAP_SERVER}" "${KAFKA_CLIENT_CONFIG}" "${KAFKA_PREWARM_EVIDENCE_FILE}" &
child_pid=$!
wait "${child_pid}"
status=$?
child_pid=""
if [[ "${status}" -ne 0 && ! -s "${KAFKA_PREWARM_EVIDENCE_FILE}" ]]; then
  fallback="{\"outcome\":\"failed\",\"reason\":\"prewarm_process_exit_${status}\"}"
  printf '%s\n' "${fallback}" > "${KAFKA_PREWARM_EVIDENCE_FILE}"
  printf 'warmup-kafka-prewarm evidence=%s\n' "${fallback}" >&2
fi
exit "${status}"
