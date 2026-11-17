#!/usr/bin/env bash

set -uo pipefail

runtime_dir="${KAFKA_RUNTIME_DIR:-/var/run/ckc-warmup-kafka}"
ready_file="${KAFKA_READY_FILE:-${runtime_dir}/ready}"
broker_host="${KAFKA_READINESS_HOST:-127.0.0.1}"
broker_port="${KAFKA_READINESS_PORT:-19092}"
connect_timeout="${KAFKA_READINESS_TIMEOUT_SECONDS:-2}"

[[ "${broker_port}" =~ ^[1-9][0-9]*$ ]] || exit 1
(( broker_port <= 65535 )) || exit 1
[[ "${connect_timeout}" =~ ^[1-9][0-9]*$ ]] || exit 1
[[ -s "${ready_file}" ]] || exit 1
pid_file="${runtime_dir}/broker.pid"
[[ -s "${pid_file}" ]] || exit 1
read -r pid < "${pid_file}" || exit 1
[[ "${pid}" =~ ^[1-9][0-9]*$ ]] || exit 1
kill -0 "${pid}" 2>/dev/null || exit 1

timeout "${connect_timeout}" bash -c ">/dev/tcp/${broker_host}/${broker_port}" 2>/dev/null
