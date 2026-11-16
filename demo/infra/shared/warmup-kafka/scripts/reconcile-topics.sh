#!/usr/bin/env bash

set -uo pipefail

KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:19092}"
KAFKA_BOOTSTRAP_TOPICS="${KAFKA_BOOTSTRAP_TOPICS:-}"
KAFKA_RECONCILE_INTERVAL_SECONDS="${KAFKA_RECONCILE_INTERVAL_SECONDS:-5}"
KAFKA_RECONCILE_BACKOFF_SECONDS="${KAFKA_RECONCILE_BACKOFF_SECONDS:-2}"
KAFKA_OPERATION_TIMEOUT_SECONDS="${KAFKA_OPERATION_TIMEOUT_SECONDS:-10}"
KAFKA_RECONCILE_ONCE="${KAFKA_RECONCILE_ONCE:-false}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-/var/run/ckc-warmup-kafka/client.properties}"
KAFKA_READY_FILE="${KAFKA_READY_FILE:-/var/run/ckc-warmup-kafka/ready}"

log() {
  printf '%s warmup-kafka-reconcile level=%s event=%s %s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "${3:-}" >&2
}

positive_integer() {
  [[ "$2" =~ ^[1-9][0-9]*$ ]] || {
    log error invalid_configuration "$1 must be a positive integer: $2"
    exit 2
  }
}

positive_integer KAFKA_RECONCILE_INTERVAL_SECONDS "${KAFKA_RECONCILE_INTERVAL_SECONDS}"
positive_integer KAFKA_RECONCILE_BACKOFF_SECONDS "${KAFKA_RECONCILE_BACKOFF_SECONDS}"
positive_integer KAFKA_OPERATION_TIMEOUT_SECONDS "${KAFKA_OPERATION_TIMEOUT_SECONDS}"
if [[ "${KAFKA_RECONCILE_ONCE}" != "true" && "${KAFKA_RECONCILE_ONCE}" != "false" ]]; then
  log error invalid_configuration "KAFKA_RECONCILE_ONCE must be true or false"
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
printf 'request.timeout.ms=%s\ndefault.api.timeout.ms=%s\n' \
  "$((KAFKA_OPERATION_TIMEOUT_SECONDS * 1000))" "$((KAFKA_OPERATION_TIMEOUT_SECONDS * 1000))" \
  > "${KAFKA_CLIENT_CONFIG}"

run_kafka() {
  timeout --signal=TERM --kill-after=2 "${KAFKA_OPERATION_TIMEOUT_SECONDS}" \
    "$@" --command-config "${KAFKA_CLIENT_CONFIG}"
}

broker_available() {
  run_kafka "${KAFKA_BIN_DIR}/kafka-broker-api-versions.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" >/dev/null 2>&1
}

topic_partition_count() {
  local topic="$1" description partitions pattern
  pattern="\\Q${topic}\\E"
  if ! description="$(run_kafka "${KAFKA_BIN_DIR}/kafka-topics.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" --describe --topic "${pattern}" 2>&1)"; then
    return 1
  fi
  partitions="$(printf '%s\n' "${description}" | awk -v expected="${topic}" '
    $1 == "Topic:" && $2 == expected {
      for (field = 3; field <= NF; field++) {
        if ($field == "PartitionCount:") {
          print $(field + 1)
          exit
        }
      }
    }
  ')"
  [[ "${partitions}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${partitions}"
}

reconcile_topic() {
  local topic="$1" partitions
  if ! partitions="$(topic_partition_count "${topic}")"; then
    log info topic_creation "topic=${topic}"
    if ! run_kafka "${KAFKA_BIN_DIR}/kafka-topics.sh" \
      --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" --create --if-not-exists \
      --topic "${topic}" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
      log warn topic_creation_failed "topic=${topic}"
      return 1
    fi
    if ! partitions="$(topic_partition_count "${topic}")"; then
      log warn metadata_unavailable "topic=${topic}"
      return 1
    fi
  fi
  if [[ "${partitions}" != "1" ]]; then
    rm -f -- "${KAFKA_READY_FILE}"
    log error incompatible_metadata "topic=${topic} partitions=${partitions} expected=1"
    return 1
  fi
}

mark_ready() {
  local temporary="${KAFKA_READY_FILE}.$$"
  printf '%s\n' "$(date +%s)" > "${temporary}"
  mv -f -- "${temporary}" "${KAFKA_READY_FILE}"
}

while :; do
  if ! broker_available; then
    rm -f -- "${KAFKA_READY_FILE}"
    log warn broker_unavailable "bootstrap=${KAFKA_BOOTSTRAP_SERVER}"
    sleep "${KAFKA_RECONCILE_BACKOFF_SECONDS}"
    continue
  fi

  compatible=true
  for topic in "${topics[@]}"; do
    if ! reconcile_topic "${topic}"; then
      compatible=false
    fi
  done
  if [[ "${compatible}" == "true" ]]; then
    mark_ready
    log info topics_ready "count=${#topics[@]}"
    if [[ "${KAFKA_RECONCILE_ONCE}" == "true" ]]; then
      exit 0
    fi
    sleep "${KAFKA_RECONCILE_INTERVAL_SECONDS}"
  else
    rm -f -- "${KAFKA_READY_FILE}"
    sleep "${KAFKA_RECONCILE_BACKOFF_SECONDS}"
  fi
done
