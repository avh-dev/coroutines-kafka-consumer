#!/usr/bin/env bash

set -uo pipefail

KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:19092}"
KAFKA_BOOTSTRAP_TOPICS="${KAFKA_BOOTSTRAP_TOPICS:-}"
KAFKA_BOOTSTRAP_TIMEOUT_SECONDS="${KAFKA_BOOTSTRAP_TIMEOUT_SECONDS:-120}"
KAFKA_BOOTSTRAP_BACKOFF_SECONDS="${KAFKA_BOOTSTRAP_BACKOFF_SECONDS:-2}"
KAFKA_OPERATION_TIMEOUT_SECONDS="${KAFKA_OPERATION_TIMEOUT_SECONDS:-10}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-/var/run/ckc-warmup-kafka/client.properties}"
KAFKA_READY_FILE="${KAFKA_READY_FILE:-/var/run/ckc-warmup-kafka/ready}"
KAFKA_PREWARM_ENABLED="${KAFKA_PREWARM_ENABLED:-true}"
KAFKA_PREWARM_COMMAND="${KAFKA_PREWARM_COMMAND:-/opt/ckc-warmup-kafka/bin/prewarm-broker.sh}"
prewarm_pid=""

terminate_prewarm() {
  if [[ -n "${prewarm_pid}" ]] && kill -0 "${prewarm_pid}" 2>/dev/null; then
    kill -TERM "${prewarm_pid}" 2>/dev/null || true
    wait "${prewarm_pid}" 2>/dev/null || true
  fi
  exit 130
}
trap terminate_prewarm TERM INT HUP

log() {
  printf '%s warmup-kafka-bootstrap level=%s event=%s %s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "${3:-}" >&2
}

positive_integer() {
  [[ "$2" =~ ^[1-9][0-9]*$ ]] || {
    log error invalid_configuration "$1 must be a positive integer: $2"
    exit 2
  }
}

positive_integer KAFKA_BOOTSTRAP_TIMEOUT_SECONDS "${KAFKA_BOOTSTRAP_TIMEOUT_SECONDS}"
positive_integer KAFKA_BOOTSTRAP_BACKOFF_SECONDS "${KAFKA_BOOTSTRAP_BACKOFF_SECONDS}"
positive_integer KAFKA_OPERATION_TIMEOUT_SECONDS "${KAFKA_OPERATION_TIMEOUT_SECONDS}"
if [[ "${KAFKA_PREWARM_ENABLED}" != "true" && "${KAFKA_PREWARM_ENABLED}" != "false" ]]; then
  log error invalid_configuration "KAFKA_PREWARM_ENABLED must be true or false: ${KAFKA_PREWARM_ENABLED}"
  exit 2
fi

declare -a topics=()
declare -A seen_topics=()
IFS=',' read -ra configured_topics <<< "${KAFKA_BOOTSTRAP_TOPICS}"
for configured in "${configured_topics[@]}"; do
  topic="${configured#"${configured%%[![:space:]]*}"}"
  topic="${topic%"${topic##*[![:space:]]}"}"
  if [[ -z "${topic}" ]]; then
    log error invalid_configuration "KAFKA_BOOTSTRAP_TOPICS contains an empty topic name"
    exit 2
  fi
  if [[ ${#topic} -gt 249 || "${topic}" == "." || "${topic}" == ".." || ! "${topic}" =~ ^[a-zA-Z0-9._-]+$ ]]; then
    log error invalid_configuration "invalid Kafka topic name: ${topic}"
    exit 2
  fi
  if [[ -z "${seen_topics[${topic}]+configured}" ]]; then
    topics+=("${topic}")
    seen_topics["${topic}"]=1
  fi
done
if [[ ${#topics[@]} -eq 0 ]]; then
  log error invalid_configuration "KAFKA_BOOTSTRAP_TOPICS must contain at least one topic"
  exit 2
fi

mkdir -p "$(dirname -- "${KAFKA_READY_FILE}")" "$(dirname -- "${KAFKA_CLIENT_CONFIG}")"
rm -f -- "${KAFKA_READY_FILE}"
printf 'request.timeout.ms=%s\ndefault.api.timeout.ms=%s\n' \
  "$((KAFKA_OPERATION_TIMEOUT_SECONDS * 1000))" "$((KAFKA_OPERATION_TIMEOUT_SECONDS * 1000))" \
  > "${KAFKA_CLIENT_CONFIG}"

run_kafka() {
  timeout --signal=TERM --kill-after=2 "${KAFKA_OPERATION_TIMEOUT_SECONDS}" \
    "$@" --command-config "${KAFKA_CLIENT_CONFIG}"
}

describe_topics() {
  run_kafka "${KAFKA_BIN_DIR}/kafka-topics.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" --describe
}

topic_partition_count() {
  local description="$1" topic="$2"
  printf '%s\n' "${description}" | awk -v expected="${topic}" '
    $1 == "Topic:" && $2 == expected {
      for (field = 3; field <= NF; field++) {
        if ($field == "PartitionCount:") {
          print $(field + 1)
          exit
        }
      }
    }
  '
}

deadline=$((SECONDS + KAFKA_BOOTSTRAP_TIMEOUT_SECONDS))
description=""
while ! description="$(describe_topics 2>/dev/null)"; do
  if (( SECONDS >= deadline )); then
    log error bootstrap_timeout "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
    exit 1
  fi
  log warn broker_unavailable "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
  sleep "${KAFKA_BOOTSTRAP_BACKOFF_SECONDS}"
done

created=false
for topic in "${topics[@]}"; do
  partitions="$(topic_partition_count "${description}" "${topic}")"
  if [[ -z "${partitions}" ]]; then
    log info topic_creation "topic=${topic}"
    if ! run_kafka "${KAFKA_BIN_DIR}/kafka-topics.sh" \
      --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" --create --if-not-exists \
      --topic "${topic}" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
      log error topic_creation_failed "topic=${topic}"
      exit 1
    fi
    created=true
  elif [[ "${partitions}" != "1" ]]; then
    log error incompatible_metadata "topic=${topic} partitions=${partitions} expected=1"
    exit 1
  fi
done

# A single final metadata snapshot proves that newly created and pre-existing topics
# expose the exact configuration required by the protocol.
if [[ "${created}" == "true" ]]; then
  if ! description="$(describe_topics 2>/dev/null)"; then
    log error metadata_unavailable "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
    exit 1
  fi
fi
for topic in "${topics[@]}"; do
  partitions="$(topic_partition_count "${description}" "${topic}")"
  if [[ -z "${partitions}" ]]; then
    log error metadata_unavailable "topic=${topic}"
    exit 1
  fi
  if [[ "${partitions}" != "1" ]]; then
    log error incompatible_metadata "topic=${topic} partitions=${partitions} expected=1"
    exit 1
  fi
done

if [[ "${KAFKA_PREWARM_ENABLED}" == "true" ]]; then
  log info broker_prewarm_started "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
  "${KAFKA_PREWARM_COMMAND}" &
  prewarm_pid=$!
  if ! wait "${prewarm_pid}"; then
    prewarm_pid=""
    log error broker_prewarm_failed "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
    exit 1
  fi
  prewarm_pid=""
  log info broker_prewarm_completed "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
fi

printf '%s\n' "$(date +%s)" > "${KAFKA_READY_FILE}"
log info topics_ready "count=${#topics[@]}"
