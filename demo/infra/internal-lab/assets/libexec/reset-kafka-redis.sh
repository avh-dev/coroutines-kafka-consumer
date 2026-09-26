#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
TOPIC_SPECS="${TOPIC_SPECS:-order.events.v1:4,batch.events.v1:4,cauldron.events.v1:4}"
CONSUMER_GROUPS="${CONSUMER_GROUPS:-ckc-demo}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-ckc-perf-redpanda}"
APACHE_KAFKA_CONTAINER="${APACHE_KAFKA_CONTAINER:-ckc-perf-kafka}"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC_RETENTION_MS="${TOPIC_RETENTION_MS:-3600000}"
TOPIC_SEGMENT_MS="${TOPIC_SEGMENT_MS:-60000}"
TOPIC_RETENTION_BYTES="${TOPIC_RETENTION_BYTES:-}"
REQUESTED_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-}"
REQUESTED_KAFKA_TOPOLOGY="${LAB_KAFKA_TOPOLOGY:-}"
REQUESTED_KAFKA_BROKER_COUNT="${LAB_KAFKA_BROKER_COUNT:-}"
REQUESTED_KAFKA_REPLICATION_FACTOR="${LAB_KAFKA_REPLICATION_FACTOR:-}"
REQUESTED_KAFKA_MIN_INSYNC_REPLICAS="${LAB_KAFKA_MIN_INSYNC_REPLICAS:-}"
REQUESTED_KAFKA_CPU_PER_BROKER="${LAB_KAFKA_CPU_PER_BROKER:-}"
REQUESTED_KAFKA_MEMORY_PER_BROKER="${LAB_KAFKA_MEMORY_PER_BROKER:-}"
REQUESTED_KAFKA_HEAP_PER_BROKER="${LAB_KAFKA_HEAP_PER_BROKER:-}"
REQUESTED_KAFKA_MEMORY_RUNTIME="${LAB_KAFKA_MEMORY_RUNTIME:-}"
REQUESTED_KAFKA_HEAP_RUNTIME="${LAB_KAFKA_HEAP_RUNTIME:-}"
KAFKA_TOPIC_METADATA_FILE="${KAFKA_TOPIC_METADATA_FILE:-}"

if [ -f "${LAB_ENV}" ]; then
  # shellcheck disable=SC1090
  . "${LAB_ENV}"
fi
LAB_KAFKA_IMPLEMENTATION="${REQUESTED_KAFKA_IMPLEMENTATION:-${LAB_KAFKA_IMPLEMENTATION:-apache-kafka}}"
LAB_KAFKA_TOPOLOGY="${REQUESTED_KAFKA_TOPOLOGY:-${LAB_KAFKA_TOPOLOGY:-single}}"
LAB_KAFKA_BROKER_COUNT="${REQUESTED_KAFKA_BROKER_COUNT:-${LAB_KAFKA_BROKER_COUNT:-}}"
LAB_KAFKA_REPLICATION_FACTOR="${REQUESTED_KAFKA_REPLICATION_FACTOR:-${LAB_KAFKA_REPLICATION_FACTOR:-}}"
LAB_KAFKA_MIN_INSYNC_REPLICAS="${REQUESTED_KAFKA_MIN_INSYNC_REPLICAS:-${LAB_KAFKA_MIN_INSYNC_REPLICAS:-}}"
LAB_KAFKA_CPU_PER_BROKER="${REQUESTED_KAFKA_CPU_PER_BROKER:-${LAB_KAFKA_CPU_PER_BROKER:-}}"
LAB_KAFKA_MEMORY_PER_BROKER="${REQUESTED_KAFKA_MEMORY_PER_BROKER:-${LAB_KAFKA_MEMORY_PER_BROKER:-}}"
LAB_KAFKA_HEAP_PER_BROKER="${REQUESTED_KAFKA_HEAP_PER_BROKER:-${LAB_KAFKA_HEAP_PER_BROKER:-}}"
LAB_KAFKA_MEMORY_RUNTIME="${REQUESTED_KAFKA_MEMORY_RUNTIME:-${LAB_KAFKA_MEMORY_RUNTIME:-}}"
LAB_KAFKA_HEAP_RUNTIME="${REQUESTED_KAFKA_HEAP_RUNTIME:-${LAB_KAFKA_HEAP_RUNTIME:-}}"
if [ -z "${LAB_NODE_IP:-}" ]; then
  echo "LAB_NODE_IP is required in ${LAB_ENV}." >&2
  exit 1
fi

normalize_kafka_implementation() {
  case "$1" in
    redpanda|rp) printf "%s\n" "redpanda" ;;
    apache-kafka|apache|kafka) printf "%s\n" "apache-kafka" ;;
    *)
      echo "Unsupported LAB_KAFKA_IMPLEMENTATION: $1" >&2
      echo "Expected redpanda or apache-kafka." >&2
      exit 1
      ;;
  esac
}

LAB_KAFKA_IMPLEMENTATION="$(normalize_kafka_implementation "${LAB_KAFKA_IMPLEMENTATION}")"
case "${LAB_KAFKA_TOPOLOGY}" in
  single) ;;
  cluster|three-node) LAB_KAFKA_TOPOLOGY="cluster" ;;
  *) echo "Expected LAB_KAFKA_TOPOLOGY to be single or cluster: ${LAB_KAFKA_TOPOLOGY}" >&2; exit 1 ;;
esac
if [ "${LAB_KAFKA_IMPLEMENTATION}" = "redpanda" ] && [ "${LAB_KAFKA_TOPOLOGY}" != "single" ]; then
  echo "LAB_KAFKA_TOPOLOGY=cluster requires LAB_KAFKA_IMPLEMENTATION=apache-kafka." >&2
  exit 1
fi

kafka_runtime_memory() {
  case "$1" in
    *Mi) amount="${1%Mi}"; suffix="m" ;;
    *Gi) amount="${1%Gi}"; suffix="g" ;;
    *) echo "Kafka memory values must use Mi or Gi: $1" >&2; exit 1 ;;
  esac
  case "${amount}" in
    ''|0|*[!0-9]*) echo "Kafka memory values must be positive integers: $1" >&2; exit 1 ;;
  esac
  printf '%s%s\n' "${amount}" "${suffix}"
}

if [ "${LAB_KAFKA_TOPOLOGY}" = "cluster" ]; then
  LAB_KAFKA_MEMORY_PER_BROKER="${LAB_KAFKA_MEMORY_PER_BROKER:-2Gi}"
  LAB_KAFKA_HEAP_PER_BROKER="${LAB_KAFKA_HEAP_PER_BROKER:-1Gi}"
else
  LAB_KAFKA_MEMORY_PER_BROKER="${LAB_KAFKA_MEMORY_PER_BROKER:-4Gi}"
  LAB_KAFKA_HEAP_PER_BROKER="${LAB_KAFKA_HEAP_PER_BROKER:-2Gi}"
fi
LAB_KAFKA_MEMORY_RUNTIME="${LAB_KAFKA_MEMORY_RUNTIME:-$(kafka_runtime_memory "${LAB_KAFKA_MEMORY_PER_BROKER}")}"
LAB_KAFKA_HEAP_RUNTIME="${LAB_KAFKA_HEAP_RUNTIME:-$(kafka_runtime_memory "${LAB_KAFKA_HEAP_PER_BROKER}")}"
if [ "${LAB_KAFKA_IMPLEMENTATION}" = "redpanda" ]; then
  KAFKA_SERVICES="redpanda"
  KAFKA_CONTAINERS="${REDPANDA_CONTAINER}"
  KAFKA_TOPIC_REPLICATION_FACTOR="${LAB_KAFKA_REPLICATION_FACTOR:-1}"
  KAFKA_TOPIC_MIN_ISR="${LAB_KAFKA_MIN_INSYNC_REPLICAS:-1}"
elif [ "${LAB_KAFKA_TOPOLOGY}" = "cluster" ]; then
  KAFKA_SERVICES="apache-kafka-1 apache-kafka-2 apache-kafka-3"
  KAFKA_CONTAINERS="ckc-perf-kafka-1 ckc-perf-kafka-2 ckc-perf-kafka-3"
  APACHE_KAFKA_CONTAINER="ckc-perf-kafka-1"
  KAFKA_TOPIC_REPLICATION_FACTOR="${LAB_KAFKA_REPLICATION_FACTOR:-3}"
  KAFKA_TOPIC_MIN_ISR="${LAB_KAFKA_MIN_INSYNC_REPLICAS:-2}"
else
  KAFKA_SERVICES="apache-kafka"
  KAFKA_CONTAINERS="${APACHE_KAFKA_CONTAINER}"
  KAFKA_TOPIC_REPLICATION_FACTOR="${LAB_KAFKA_REPLICATION_FACTOR:-1}"
  KAFKA_TOPIC_MIN_ISR="${LAB_KAFKA_MIN_INSYNC_REPLICAS:-1}"
fi
EXPECTED_BROKER_COUNT=1
[ "${LAB_KAFKA_TOPOLOGY}" = "cluster" ] && EXPECTED_BROKER_COUNT=3
LAB_KAFKA_BROKER_COUNT="${LAB_KAFKA_BROKER_COUNT:-${EXPECTED_BROKER_COUNT}}"
case "${LAB_KAFKA_BROKER_COUNT}:${KAFKA_TOPIC_REPLICATION_FACTOR}:${KAFKA_TOPIC_MIN_ISR}" in
  *[!0-9:]*|*::*|:*) echo "Kafka broker, replication, and min ISR settings must be positive integers." >&2; exit 1 ;;
esac
if [ "${LAB_KAFKA_BROKER_COUNT}" -ne "${EXPECTED_BROKER_COUNT}" ] \
  || [ "${KAFKA_TOPIC_REPLICATION_FACTOR}" -lt 1 ] \
  || [ "${KAFKA_TOPIC_MIN_ISR}" -lt 1 ] \
  || [ "${KAFKA_TOPIC_REPLICATION_FACTOR}" -gt "${LAB_KAFKA_BROKER_COUNT}" ] \
  || [ "${KAFKA_TOPIC_MIN_ISR}" -gt "${KAFKA_TOPIC_REPLICATION_FACTOR}" ]; then
  echo "Invalid Kafka shape: topology=${LAB_KAFKA_TOPOLOGY}, brokers=${LAB_KAFKA_BROKER_COUNT}, replication=${KAFKA_TOPIC_REPLICATION_FACTOR}, min_isr=${KAFKA_TOPIC_MIN_ISR}." >&2
  exit 1
fi

rpk() {
  docker exec "${REDPANDA_CONTAINER}" rpk -X "brokers=${BOOTSTRAP_SERVER}" "$@"
}

apache_kafka_topics() {
  docker exec "${APACHE_KAFKA_CONTAINER}" env KAFKA_OPTS= /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" "$@"
}

apache_kafka_groups() {
  docker exec "${APACHE_KAFKA_CONTAINER}" env KAFKA_OPTS= /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "${BOOTSTRAP_SERVER}" "$@"
}

delete_group() {
  case "${LAB_KAFKA_IMPLEMENTATION}" in
    redpanda) rpk group delete "$1" || true ;;
    apache-kafka) apache_kafka_groups --delete --group "$1" || true ;;
  esac
}

delete_topic() {
  case "${LAB_KAFKA_IMPLEMENTATION}" in
    redpanda) rpk topic delete "$1" || true ;;
    apache-kafka) apache_kafka_topics --delete --topic "$1" || true ;;
  esac
}

topic_exists() {
  case "${LAB_KAFKA_IMPLEMENTATION}" in
    redpanda) rpk topic list 2>/dev/null | awk -v topic="$1" '$1 == topic { found=1 } END { exit found ? 0 : 1 }' ;;
    apache-kafka) apache_kafka_topics --list 2>/dev/null | grep -Fxq "$1" ;;
  esac
}

wait_topic_deleted() {
  topic="$1"

  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
    if ! topic_exists "${topic}"; then
      return
    fi
    sleep 1
  done

  echo "Topic ${topic} was not deleted in time." >&2
  exit 1
}

create_topic() {
  topic="$1"
  partitions="$2"
  case "${LAB_KAFKA_IMPLEMENTATION}" in
    redpanda)
      if [ -n "${TOPIC_RETENTION_BYTES}" ]; then
        rpk topic create "${topic}" -p "${partitions}" -r "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
          -c "retention.ms=${TOPIC_RETENTION_MS}" \
          -c "segment.ms=${TOPIC_SEGMENT_MS}" \
          -c "retention.bytes=${TOPIC_RETENTION_BYTES}"
      else
        rpk topic create "${topic}" -p "${partitions}" -r "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
          -c "retention.ms=${TOPIC_RETENTION_MS}" \
          -c "segment.ms=${TOPIC_SEGMENT_MS}"
      fi
      ;;
    apache-kafka)
      if [ -n "${TOPIC_RETENTION_BYTES}" ]; then
        apache_kafka_topics --create --topic "${topic}" --partitions "${partitions}" --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
          --config "min.insync.replicas=${KAFKA_TOPIC_MIN_ISR}" \
          --config "retention.ms=${TOPIC_RETENTION_MS}" \
          --config "segment.ms=${TOPIC_SEGMENT_MS}" \
          --config "retention.bytes=${TOPIC_RETENTION_BYTES}"
      else
        apache_kafka_topics --create --topic "${topic}" --partitions "${partitions}" --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
          --config "min.insync.replicas=${KAFKA_TOPIC_MIN_ISR}" \
          --config "retention.ms=${TOPIC_RETENTION_MS}" \
          --config "segment.ms=${TOPIC_SEGMENT_MS}"
      fi
      ;;
  esac
}

list_topics() {
  case "${LAB_KAFKA_IMPLEMENTATION}" in
    redpanda) rpk topic list ;;
    apache-kafka) apache_kafka_topics --list ;;
  esac
}

topic_description() {
  case "${LAB_KAFKA_IMPLEMENTATION}" in
    redpanda) rpk topic describe "$1" ;;
    apache-kafka) apache_kafka_topics --describe --topic "$1" ;;
  esac
}

write_topic_metadata() {
  [ -n "${KAFKA_TOPIC_METADATA_FILE}" ] || return
  metadata_dir="$(dirname "${KAFKA_TOPIC_METADATA_FILE}")"
  mkdir -p "${metadata_dir}"
  records_file="${KAFKA_TOPIC_METADATA_FILE}.records"
  : > "${records_file}"
  previous_ifs="${IFS-}"
  IFS=","
  for spec in ${TOPIC_SPECS}; do
    topic="${spec%:*}"
    expected_partitions="${spec##*:}"
    description="$(topic_description "${topic}")"
    topic_id="$(printf '%s\n' "${description}" | sed -n 's/.*TopicId: \([^[:space:]]*\).*/\1/p' | head -n 1)"
    actual_partitions="$(printf '%s\n' "${description}" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p' | head -n 1)"
    actual_replication_factor="$(printf '%s\n' "${description}" | sed -n 's/.*ReplicationFactor: \([0-9][0-9]*\).*/\1/p' | head -n 1)"
    actual_min_isr="$(printf '%s\n' "${description}" | sed -n 's/.*min\.insync\.replicas=\([0-9][0-9]*\).*/\1/p' | head -n 1)"
    printf '%s|%s|%s|%s|%s\n' "${topic}" "${topic_id}" "${actual_partitions:-${expected_partitions}}" "${actual_replication_factor:-${KAFKA_TOPIC_REPLICATION_FACTOR}}" "${actual_min_isr:-${KAFKA_TOPIC_MIN_ISR}}" >> "${records_file}"
  done
  IFS="${previous_ifs}"
  KAFKA_TOPIC_METADATA_RECORDS="${records_file}" KAFKA_TOPIC_METADATA_OUTPUT="${KAFKA_TOPIC_METADATA_FILE}" python3 - <<'PY'
import json
import os
from datetime import datetime, timezone
from pathlib import Path

records = []
for line in Path(os.environ["KAFKA_TOPIC_METADATA_RECORDS"]).read_text(encoding="utf-8").splitlines():
    name, topic_id, partitions, replication_factor, min_isr = line.split("|", 4)
    records.append({
        "name": name,
        "id": topic_id or None,
        "partitions": int(partitions),
        "replication_factor": int(replication_factor),
        "min_insync_replicas": int(min_isr),
    })
Path(os.environ["KAFKA_TOPIC_METADATA_OUTPUT"]).write_text(
    json.dumps({"schema_version": 1, "captured_at": datetime.now(timezone.utc).isoformat(), "topics": records}, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
  rm -f "${records_file}"
}

kafka_runtime_signature() {
  printf '%s|%s\n' "${LAB_KAFKA_IMPLEMENTATION}" "${LAB_KAFKA_TOPOLOGY}"
  for container in ${KAFKA_CONTAINERS}; do
    if docker inspect "${container}" >/dev/null 2>&1; then
      docker inspect --format '{{.Name}}|{{.Id}}|{{.State.Running}}|{{.State.StartedAt}}' "${container}"
    else
      printf '/%s|missing\n' "${container}"
    fi
  done
}

record_kafka_runtime_signature() {
  signature="$1"
  mkdir -p "$(dirname "${KAFKA_RUNTIME_SIGNATURE_FILE}")"
  temporary_signature="${KAFKA_RUNTIME_SIGNATURE_FILE}.tmp.$$"
  printf '%s\n' "${signature}" > "${temporary_signature}"
  mv "${temporary_signature}" "${KAFKA_RUNTIME_SIGNATURE_FILE}"
}

warm_apache_kafka() {
  reason="$1"
  set -- \
    --backend docker \
    --docker-container "${APACHE_KAFKA_CONTAINER}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
    --reason "${reason}" \
    --experiment "${EXPERIMENT_NAME:-internal-lab experiment}" \
    --log-file "${LAB_ROOT}/logs/kafka-warmup.log"
  if [ -n "${CKC_NOTIFY_HOOK:-}" ]; then
    set -- "$@" --notify-hook "${CKC_NOTIFY_HOOK}" \
      --notification-dir "${CKC_NOTIFICATION_DIR:-${LAB_ROOT}/state/notifications}"
  fi
  python3 "${LAB_ROOT}/helpers/kafka_warmup/run.py" "$@"
}

KAFKA_RUNTIME_SIGNATURE_FILE="${LAB_ROOT}/state/kafka-runtime.signature"
KAFKA_RUNTIME_BEFORE="$(kafka_runtime_signature)"
KAFKA_RUNTIME_PREVIOUS="$(cat "${KAFKA_RUNTIME_SIGNATURE_FILE}" 2>/dev/null || true)"

if [ "${LAB_KAFKA_IMPLEMENTATION}" = "redpanda" ]; then
  docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" rm -f -s apache-kafka apache-kafka-1 apache-kafka-2 apache-kafka-3 >/dev/null 2>&1 || true
else
  docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" rm -f -s redpanda >/dev/null 2>&1 || true
  if [ "${LAB_KAFKA_TOPOLOGY}" = "cluster" ]; then
    docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" rm -f -s apache-kafka >/dev/null 2>&1 || true
  else
    docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" rm -f -s apache-kafka-1 apache-kafka-2 apache-kafka-3 >/dev/null 2>&1 || true
  fi
fi
LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}" \
LAB_KAFKA_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR}" LAB_KAFKA_MIN_INSYNC_REPLICAS="${KAFKA_TOPIC_MIN_ISR}" \
LAB_KAFKA_CPU_PER_BROKER="${LAB_KAFKA_CPU_PER_BROKER}" LAB_KAFKA_MEMORY_RUNTIME="${LAB_KAFKA_MEMORY_RUNTIME}" LAB_KAFKA_HEAP_RUNTIME="${LAB_KAFKA_HEAP_RUNTIME}" \
  docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" up -d --wait ${KAFKA_SERVICES} redis
LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}" \
  docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" up -d --no-deps --force-recreate kafka-exporter process-exporter >/dev/null 2>&1 || true
KAFKA_RUNTIME_AFTER="$(kafka_runtime_signature)"
KAFKA_WARMUP_REASON=""
if [ "${KAFKA_RUNTIME_BEFORE}" != "${KAFKA_RUNTIME_AFTER}" ]; then
  KAFKA_WARMUP_REASON="Kafka broker containers were created or replaced"
elif [ -n "${KAFKA_RUNTIME_PREVIOUS}" ] && [ "${KAFKA_RUNTIME_PREVIOUS}" != "${KAFKA_RUNTIME_AFTER}" ]; then
  KAFKA_WARMUP_REASON="Kafka broker containers restarted since the previous target"
fi
if [ -n "${KAFKA_WARMUP_REASON}" ] && [ "${LAB_KAFKA_IMPLEMENTATION}" = "apache-kafka" ]; then
  if [ -n "${EXPERIMENT_PROGRESS_FILE:-}" ]; then
    python3 "${LAB_ROOT}/helpers/experiment_progress.py" \
      --file "${EXPERIMENT_PROGRESS_FILE}" --step warming_kafka --label "warming Kafka" >/dev/null 2>&1 || true
  fi
  warm_apache_kafka "${KAFKA_WARMUP_REASON}"
  if [ -n "${EXPERIMENT_PROGRESS_FILE:-}" ]; then
    python3 "${LAB_ROOT}/helpers/experiment_progress.py" \
      --file "${EXPERIMENT_PROGRESS_FILE}" --step preparing_target --label "preparing target" >/dev/null 2>&1 || true
  fi
fi
record_kafka_runtime_signature "${KAFKA_RUNTIME_AFTER}"
docker exec ckc-perf-redis redis-cli FLUSHALL

IFS=","
for group in ${CONSUMER_GROUPS}; do
  delete_group "${group}"
done

IFS=","
for spec in ${TOPIC_SPECS}; do
  topic="${spec%:*}"
  delete_topic "${topic}"
done

for spec in ${TOPIC_SPECS}; do
  topic="${spec%:*}"
  wait_topic_deleted "${topic}"
  partitions="${spec##*:}"
  create_topic "${topic}" "${partitions}"
done
unset IFS

write_topic_metadata
list_topics
