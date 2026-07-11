#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
TOPIC_SPECS="${TOPIC_SPECS:-order.events.v1:4,batch.events.v1:4,cauldron.events.v1:4}"
CONSUMER_GROUPS="${CONSUMER_GROUPS:-potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons,spring-kafka-order-lifecycle,spring-kafka-batch-lifecycle,spring-kafka-cauldron-telemetry}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-ckc-perf-redpanda}"
APACHE_KAFKA_CONTAINER="${APACHE_KAFKA_CONTAINER:-ckc-perf-kafka}"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC_RETENTION_MS="${TOPIC_RETENTION_MS:-300000}"
TOPIC_SEGMENT_MS="${TOPIC_SEGMENT_MS:-60000}"
TOPIC_RETENTION_BYTES="${TOPIC_RETENTION_BYTES:-}"
REQUESTED_KAFKA_IMPLEMENTATION="${LAB_KAFKA_IMPLEMENTATION:-}"

if [ -f "${LAB_ENV}" ]; then
  # shellcheck disable=SC1090
  . "${LAB_ENV}"
fi
LAB_KAFKA_IMPLEMENTATION="${REQUESTED_KAFKA_IMPLEMENTATION:-${LAB_KAFKA_IMPLEMENTATION:-redpanda}}"
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
KAFKA_SERVICE="${LAB_KAFKA_IMPLEMENTATION}"

rpk() {
  docker exec "${REDPANDA_CONTAINER}" rpk -X "brokers=${BOOTSTRAP_SERVER}" "$@"
}

apache_kafka_topics() {
  docker exec "${APACHE_KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" "$@"
}

apache_kafka_groups() {
  docker exec "${APACHE_KAFKA_CONTAINER}" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "${BOOTSTRAP_SERVER}" "$@"
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
        rpk topic create "${topic}" -p "${partitions}" -r 1 \
          -c "retention.ms=${TOPIC_RETENTION_MS}" \
          -c "segment.ms=${TOPIC_SEGMENT_MS}" \
          -c "retention.bytes=${TOPIC_RETENTION_BYTES}"
      else
        rpk topic create "${topic}" -p "${partitions}" -r 1 \
          -c "retention.ms=${TOPIC_RETENTION_MS}" \
          -c "segment.ms=${TOPIC_SEGMENT_MS}"
      fi
      ;;
    apache-kafka)
      if [ -n "${TOPIC_RETENTION_BYTES}" ]; then
        apache_kafka_topics --create --topic "${topic}" --partitions "${partitions}" --replication-factor 1 \
          --config "retention.ms=${TOPIC_RETENTION_MS}" \
          --config "segment.ms=${TOPIC_SEGMENT_MS}" \
          --config "retention.bytes=${TOPIC_RETENTION_BYTES}"
      else
        apache_kafka_topics --create --topic "${topic}" --partitions "${partitions}" --replication-factor 1 \
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

if [ "${KAFKA_SERVICE}" = "redpanda" ]; then
  docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" rm -f -s apache-kafka >/dev/null 2>&1 || true
else
  docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" rm -f -s redpanda >/dev/null 2>&1 || true
fi
LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}" docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" up -d --wait "${KAFKA_SERVICE}" redis
LAB_ROOT="${LAB_ROOT}" LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}" docker compose -p ckc-internal-lab -f "${LAB_ROOT}/docker/compose/docker-compose.host-services.yml" up -d --no-deps --force-recreate kafka-exporter process-exporter >/dev/null 2>&1 || true
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

list_topics
