#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_ENV="${LAB_ROOT}/config/lab.env"
TOPIC_SPECS="${TOPIC_SPECS:-order.events.v1:4,batch.events.v1:4,cauldron.events.v1:4}"
CONSUMER_GROUPS="${CONSUMER_GROUPS:-potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons,spring-kafka-order-lifecycle,spring-kafka-batch-lifecycle,spring-kafka-cauldron-telemetry}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-ckc-perf-redpanda}"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC_RETENTION_MS="${TOPIC_RETENTION_MS:-300000}"
TOPIC_SEGMENT_MS="${TOPIC_SEGMENT_MS:-60000}"
TOPIC_RETENTION_BYTES="${TOPIC_RETENTION_BYTES:-}"

if [ -f "${LAB_ENV}" ]; then
  # shellcheck disable=SC1090
  . "${LAB_ENV}"
fi
if [ -z "${LAB_NODE_IP:-}" ]; then
  echo "LAB_NODE_IP is required in ${LAB_ENV}." >&2
  exit 1
fi

rpk() {
  docker exec "${REDPANDA_CONTAINER}" rpk -X "brokers=${BOOTSTRAP_SERVER}" "$@"
}

LAB_NODE_IP="${LAB_NODE_IP}" LAB_HOST="${LAB_HOST:-${LAB_NODE_IP}}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait kafka redis
docker exec ckc-perf-redis redis-cli FLUSHALL

IFS=","
for group in ${CONSUMER_GROUPS}; do
  rpk group delete "${group}" || true
done

IFS=","
for spec in ${TOPIC_SPECS}; do
  topic="${spec%:*}"
  rpk topic delete "${topic}" || true
done

sleep 5

for spec in ${TOPIC_SPECS}; do
  topic="${spec%:*}"
  partitions="${spec##*:}"
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
done
unset IFS

rpk topic list
