#!/usr/bin/env bash

set -euo pipefail

image="${1:-ckc-perf/warmup-kafka:latest}"
container="ckc-warmup-kafka-test-$$"
volume="ckc-warmup-kafka-test-$$"
network="ckc-warmup-kafka-test-$$"
topics="ckc.traffic-warmup.v6.control,ckc.traffic-warmup.v6.order.events,ckc.traffic-warmup.v6.batch.events,ckc.traffic-warmup.v6.cauldron.events"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
  docker volume rm "${volume}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "${network}" >/dev/null

start_broker() {
  local configured_topics="$1"
  docker run -d --name "${container}" \
    --network "${network}" --network-alias ckc-warmup-kafka \
    --mount "type=volume,source=${volume},target=/var/lib/kafka/data" \
    -e CLUSTER_ID=M8gW-iVjQkGqC1rH0vM4xA \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_LISTENERS=INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:19092,EXTERNAL://ckc-warmup-kafka:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
    -e KAFKA_BOOTSTRAP_TOPICS="${configured_topics}" \
    -e KAFKA_PREWARM_FIRST_PHASE_SECONDS=2 \
    -e KAFKA_PREWARM_SECOND_PHASE_SECONDS=2 \
    -e KAFKA_PREWARM_START_RATE=10 \
    -e KAFKA_PREWARM_MIDDLE_RATE=50 \
    -e KAFKA_PREWARM_END_RATE=100 \
    -e KAFKA_PREWARM_TIMEOUT_SECONDS=30 \
    "${image}" >/dev/null
}

wait_ready() {
  local deadline=$((SECONDS + 90))
  until docker exec "${container}" /opt/ckc-warmup-kafka/bin/readiness.sh >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      docker logs "${container}" >&2 || true
      echo "Warm-up Kafka did not become ready." >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_log() {
  local pattern="$1" deadline=$((SECONDS + 90))
  until docker logs "${container}" 2>&1 | grep -q "${pattern}"; do
    if (( SECONDS >= deadline )); then
      docker logs "${container}" >&2 || true
      echo "Warm-up Kafka did not report expected event: ${pattern}" >&2
      return 1
    fi
    sleep 1
  done
}

start_broker "${topics}"
wait_ready
prewarm_evidence="$(docker exec "${container}" cat /var/run/ckc-warmup-kafka/prewarm.json)"
grep -q '"outcome":"success"' <<< "${prewarm_evidence}"
grep -q '"accepted_records":' <<< "${prewarm_evidence}"
descriptions="$(docker exec "${container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe)"
if [[ "$(grep -c 'PartitionCount: 1' <<< "${descriptions}")" -ne 4 ]]; then
  echo "Expected four one-partition warm-up topics." >&2
  exit 1
fi
if grep -q '__ckc_warmup_broker_prewarm_' <<< "${descriptions}"; then
  echo "Temporary prewarm topic remained after readiness opened." >&2
  exit 1
fi

# Verify that the external listener returns the stable Service-like address after
# bootstrap has prepared the topics through the independent internal listener.
external_descriptions="$(docker exec "${container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server ckc-warmup-kafka:9092 --describe)"
if [[ "$(grep -c 'PartitionCount: 1' <<< "${external_descriptions}")" -ne 4 ]]; then
  echo "External listener did not expose the bootstrapped warm-up topics." >&2
  exit 1
fi

printf 'retained-record\n' | docker exec -i "${container}" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic ckc.traffic-warmup.v6.control >/dev/null
docker exec "${container}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --topic __ckc_warmup_broker_prewarm_interrupted --partitions 1 --replication-factor 1 >/dev/null
docker restart "${container}" >/dev/null
wait_ready
after_restart_topics="$(docker exec "${container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --list)"
if grep -q '^__ckc_warmup_broker_prewarm_' <<< "${after_restart_topics}"; then
  echo "Stale temporary prewarm topic survived container restart." >&2
  exit 1
fi
offsets="$(docker exec "${container}" /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:19092 --topic ckc.traffic-warmup.v6.control)"
grep -q ':1$' <<< "${offsets}"

docker exec "${container}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --topic ckc.traffic-warmup.v6.incompatible --partitions 2 --replication-factor 1 >/dev/null
docker rm -f "${container}" >/dev/null
start_broker "${topics},ckc.traffic-warmup.v6.incompatible"
wait_for_log 'incompatible_metadata.*partitions=2'
if docker exec "${container}" /opt/ckc-warmup-kafka/bin/readiness.sh >/dev/null 2>&1; then
  echo "Warm-up Kafka became ready with incompatible partition metadata." >&2
  exit 1
fi

docker rm -f "${container}" >/dev/null
docker volume rm "${volume}" >/dev/null
docker volume create "${volume}" >/dev/null
start_broker "${topics}"
wait_ready
restored="$(docker exec "${container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe)"
if [[ "$(grep -c 'PartitionCount: 1' <<< "${restored}")" -ne 4 ]]; then
  echo "Topics were not restored after complete data loss." >&2
  exit 1
fi

if docker exec "${container}" test -e /var/run/ckc-warmup-kafka/bootstrap.pid; then
  echo "Bootstrap process remained after successful topic preparation." >&2
  exit 1
fi
if ! docker exec "${container}" sh -c 'kill -0 "$(cat /var/run/ckc-warmup-kafka/broker.pid)"'; then
  echo "Broker stopped after successful topic preparation." >&2
  exit 1
fi

docker exec "${container}" sh -c 'kill "$(cat /var/run/ckc-warmup-kafka/broker.pid)"'
deadline=$((SECONDS + 15))
while [[ "$(docker inspect -f '{{.State.Running}}' "${container}")" == "true" ]]; do
  if (( SECONDS >= deadline )); then
    echo "Container stayed alive after broker stopped." >&2
    exit 1
  fi
  sleep 1
done

echo "Autonomous warm-up Kafka container lifecycle passed."
