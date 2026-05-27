#!/usr/bin/env sh

set -eu

if [ -z "${LAB_HOST_IP:-}" ]; then
  echo "LAB_HOST_IP is required." >&2
  exit 1
fi

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
TOPIC_SPECS="${TOPIC_SPECS:-order.events.v1:4,batch.events.v1:4,cauldron.events.v1:4}"
CONSUMER_GROUPS="${CONSUMER_GROUPS:-potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-ckc-perf-redpanda}"
BOOTSTRAP_SERVER="${LAB_HOST_IP}:9092"

rpk() {
  docker exec "${REDPANDA_CONTAINER}" rpk -X "brokers=${BOOTSTRAP_SERVER}" "$@"
}

LAB_HOST_IP="${LAB_HOST_IP}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait kafka redis
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
  rpk topic create "${topic}" -p "${partitions}" -r 1
done
unset IFS

rpk topic list
