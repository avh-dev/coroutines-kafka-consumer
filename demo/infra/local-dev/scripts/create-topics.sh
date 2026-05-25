#!/usr/bin/env bash

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-ckc-local-redpanda}"
RPK_BIN="${RPK_BIN:-rpk}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
DEFAULT_PARTITIONS=6

ORDER_PARTITIONS=""
BATCH_PARTITIONS=""
CAULDRON_PARTITIONS=""
HAD_ARGS=0

topic_command() {
  MSYS_NO_PATHCONV=1 docker exec "${KAFKA_CONTAINER}" "${RPK_BIN}" -X "brokers=${BOOTSTRAP_SERVER}" topic "$@"
}

usage() {
  cat <<EOF
Usage: $0 [--orders N] [--batches M] [--cauldrons K]

Creates local demo Kafka topics through rpk in ${KAFKA_CONTAINER}.
When no parameters are provided, the script prompts for both partition counts.

Options:
  --orders N      Partitions for order.events.v1. Default: ${DEFAULT_PARTITIONS}
  --batches M     Partitions for batch.events.v1. Default: ${DEFAULT_PARTITIONS}
  --cauldrons K   Partitions for cauldron.events.v1. Default: ${DEFAULT_PARTITIONS}
  --lifecycle N   Deprecated alias for --orders.
  -h, --help      Show this help.
EOF
}

require_positive_int() {
  local name="$1"
  local value="$2"

  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer, got '${value}'." >&2
    exit 1
  fi
}

prompt_partitions() {
  local label="$1"
  local value=""

  read -r -p "${label} partitions [${DEFAULT_PARTITIONS}]: " value
  echo "${value:-${DEFAULT_PARTITIONS}}"
}

current_partitions() {
  local topic="$1"
  local topics=""
  local partitions=""

  topics="$(topic_command list 2>/dev/null || true)"
  partitions="$(awk -v topic="${topic}" '$1 == topic { print $2; exit }' <<< "${topics}")"
  if [[ -z "${partitions}" ]]; then
    echo "null"
    return
  fi

  if [[ "${partitions}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${partitions}"
  else
    echo "null"
  fi
}

wait_topic_deleted() {
  local topic="$1"

  for _ in {1..30}; do
    if [[ "$(current_partitions "${topic}")" == "null" ]]; then
      return
    fi
    sleep 1
  done

  echo "Topic ${topic} was not deleted in time." >&2
  exit 1
}

recreate_topic() {
  local topic="$1"
  local partitions="$2"
  local current=""

  current="$(current_partitions "${topic}")"
  echo "${topic}: ${current} -> ${partitions}"

  if [[ "${current}" != "null" ]]; then
    topic_command delete "${topic}" >/dev/null
    wait_topic_deleted "${topic}"
  fi

  topic_command create "${topic}" -p "${partitions}" -r 1 >/dev/null
}

while [[ $# -gt 0 ]]; do
  HAD_ARGS=1
  case "$1" in
    --orders|--lifecycle)
      [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 1; }
      ORDER_PARTITIONS="$2"
      shift 2
      ;;
    --batches)
      [[ $# -ge 2 ]] || { echo "--batches requires a value." >&2; exit 1; }
      BATCH_PARTITIONS="$2"
      shift 2
      ;;
    --cauldrons)
      [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 1; }
      CAULDRON_PARTITIONS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${HAD_ARGS}" -eq 0 ]]; then
  ORDER_PARTITIONS="$(prompt_partitions "order.events.v1")"
  BATCH_PARTITIONS="$(prompt_partitions "batch.events.v1")"
  CAULDRON_PARTITIONS="$(prompt_partitions "cauldron.events.v1")"
else
  ORDER_PARTITIONS="${ORDER_PARTITIONS:-${DEFAULT_PARTITIONS}}"
  BATCH_PARTITIONS="${BATCH_PARTITIONS:-${DEFAULT_PARTITIONS}}"
  CAULDRON_PARTITIONS="${CAULDRON_PARTITIONS:-${DEFAULT_PARTITIONS}}"
fi

ORDER_PARTITIONS="${ORDER_PARTITIONS//$'\r'/}"
BATCH_PARTITIONS="${BATCH_PARTITIONS//$'\r'/}"
CAULDRON_PARTITIONS="${CAULDRON_PARTITIONS//$'\r'/}"

require_positive_int "--orders" "${ORDER_PARTITIONS}"
require_positive_int "--batches" "${BATCH_PARTITIONS}"
require_positive_int "--cauldrons" "${CAULDRON_PARTITIONS}"

recreate_topic "order.events.v1" "${ORDER_PARTITIONS}"
recreate_topic "batch.events.v1" "${BATCH_PARTITIONS}"
recreate_topic "cauldron.events.v1" "${CAULDRON_PARTITIONS}"

echo "Local Kafka topics are ready."
