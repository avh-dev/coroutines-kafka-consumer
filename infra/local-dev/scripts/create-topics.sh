#!/usr/bin/env bash

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-ckc-local-kafka}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
DEFAULT_PARTITIONS=6

LIFECYCLE_PARTITIONS=""
CAULDRONS_PARTITIONS=""
HAD_ARGS=0

topic_command() {
  MSYS_NO_PATHCONV=1 docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_BIN}" --bootstrap-server "${BOOTSTRAP_SERVER}" "$@"
}

usage() {
  cat <<EOF
Usage: $0 [--lifecycle N] [--cualdrons M]

Creates local demo Kafka topics through docker exec against ${KAFKA_CONTAINER}.
When no parameters are provided, the script prompts for both partition counts.

Options:
  --lifecycle N   Partitions for potion.orders.lifecycle.v1. Default: ${DEFAULT_PARTITIONS}
  --cualdrons M   Partitions for potion.cauldrons.telemetry.v1. Default: ${DEFAULT_PARTITIONS}
  --cauldrons M   Alias for --cualdrons.
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
  local description=""
  local partitions=""

  description="$(topic_command --describe --topic "${topic}" 2>/dev/null || true)"
  if [[ -z "${description}" ]]; then
    echo "null"
    return
  fi

  partitions="$(awk '
    /PartitionCount:/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^PartitionCount:/) {
          split($i, parts, ":")
          if (parts[2] != "") {
            print parts[2]
          } else {
            print $(i + 1)
          }
          exit
        }
      }
    }
  ' <<< "${description}")"

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
    topic_command --delete --if-exists --topic "${topic}" >/dev/null
    wait_topic_deleted "${topic}"
  fi

  topic_command \
    --create \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
}

while [[ $# -gt 0 ]]; do
  HAD_ARGS=1
  case "$1" in
    --lifecycle)
      [[ $# -ge 2 ]] || { echo "--lifecycle requires a value." >&2; exit 1; }
      LIFECYCLE_PARTITIONS="$2"
      shift 2
      ;;
    --cualdrons|--cauldrons)
      [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 1; }
      CAULDRONS_PARTITIONS="$2"
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
  LIFECYCLE_PARTITIONS="$(prompt_partitions "potion.orders.lifecycle.v1")"
  CAULDRONS_PARTITIONS="$(prompt_partitions "potion.cauldrons.telemetry.v1")"
else
  LIFECYCLE_PARTITIONS="${LIFECYCLE_PARTITIONS:-${DEFAULT_PARTITIONS}}"
  CAULDRONS_PARTITIONS="${CAULDRONS_PARTITIONS:-${DEFAULT_PARTITIONS}}"
fi

LIFECYCLE_PARTITIONS="${LIFECYCLE_PARTITIONS//$'\r'/}"
CAULDRONS_PARTITIONS="${CAULDRONS_PARTITIONS//$'\r'/}"

require_positive_int "--lifecycle" "${LIFECYCLE_PARTITIONS}"
require_positive_int "--cualdrons" "${CAULDRONS_PARTITIONS}"

recreate_topic "potion.orders.lifecycle.v1" "${LIFECYCLE_PARTITIONS}"
recreate_topic "potion.cauldrons.telemetry.v1" "${CAULDRONS_PARTITIONS}"

echo "Local Kafka topics are ready."
