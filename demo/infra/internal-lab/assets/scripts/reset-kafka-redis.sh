#!/usr/bin/env sh

set -eu

if [ -z "${LAB_HOST_IP:-}" ]; then
  echo "LAB_HOST_IP is required." >&2
  exit 1
fi

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
TOPIC_SPECS="${TOPIC_SPECS:-order.events.v1:4,batch.events.v1:4,cauldron.events.v1:4}"
CONSUMER_GROUPS="${CONSUMER_GROUPS:-potion-tracking-orders,potion-tracking-batches,potion-tracking-cauldrons}"

LAB_HOST_IP="${LAB_HOST_IP}" docker compose -f "${LAB_ROOT}/docker-compose.host-services.yml" up -d --wait kafka redis
docker exec ckc-perf-redis redis-cli FLUSHALL

IFS=","
for group in ${CONSUMER_GROUPS}; do
  docker exec ckc-perf-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "${LAB_HOST_IP}:9092" \
    --delete \
    --group "${group}" || true
done

IFS=","
for spec in ${TOPIC_SPECS}; do
  topic="${spec%:*}"
  docker exec ckc-perf-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${LAB_HOST_IP}:9092" \
    --delete --if-exists \
    --topic "${topic}" || true
done

sleep 5

for spec in ${TOPIC_SPECS}; do
  topic="${spec%:*}"
  partitions="${spec##*:}"
  docker exec ckc-perf-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${LAB_HOST_IP}:9092" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
done
unset IFS

docker exec ckc-perf-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${LAB_HOST_IP}:9092" --list
