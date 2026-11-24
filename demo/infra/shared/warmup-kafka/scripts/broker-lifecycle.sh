#!/usr/bin/env bash

set -euo pipefail

exec java -Xms16m -Xmx96m -XX:+UseSerialGC -cp '/opt/ckc-warmup-kafka/cli/*' \
  avh.ckc.warmup.broker.BrokerLifecycleCli "$@"
