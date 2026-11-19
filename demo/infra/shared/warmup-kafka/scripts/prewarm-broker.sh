#!/usr/bin/env bash

set -uo pipefail

KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:19092}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-/var/run/ckc-warmup-kafka/client.properties}"
KAFKA_PREWARM_EVIDENCE_FILE="${KAFKA_PREWARM_EVIDENCE_FILE:-/var/run/ckc-warmup-kafka/prewarm.json}"
KAFKA_PREWARM_TIMEOUT_SECONDS="${KAFKA_PREWARM_TIMEOUT_SECONDS:-180}"
child_pid=""

if ! [[ "${KAFKA_PREWARM_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KAFKA_PREWARM_TIMEOUT_SECONDS must be a positive integer: ${KAFKA_PREWARM_TIMEOUT_SECONDS}" >&2
  exit 2
fi

terminate_child() {
  if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" 2>/dev/null; then
    kill -TERM "${child_pid}" 2>/dev/null || true
  fi
}
trap terminate_child TERM INT HUP

mkdir -p "$(dirname -- "${KAFKA_PREWARM_EVIDENCE_FILE}")"
rm -f -- "${KAFKA_PREWARM_EVIDENCE_FILE}"

timeout --signal=TERM --kill-after=5 "${KAFKA_PREWARM_TIMEOUT_SECONDS}" \
  /opt/java/openjdk/bin/java -cp "/opt/ckc-warmup-kafka/classes:/opt/kafka/libs/*" \
  ckc.warmup.kafka.PrewarmBroker \
  "${KAFKA_BOOTSTRAP_SERVER}" "${KAFKA_CLIENT_CONFIG}" "${KAFKA_PREWARM_EVIDENCE_FILE}" &
child_pid=$!
wait "${child_pid}"
status=$?
child_pid=""
if [[ "${status}" -ne 0 && ! -s "${KAFKA_PREWARM_EVIDENCE_FILE}" ]]; then
  printf '{"outcome":"failed","reason":"prewarm_process_exit_%s"}\n' "${status}" \
    > "${KAFKA_PREWARM_EVIDENCE_FILE}"
fi
exit "${status}"
