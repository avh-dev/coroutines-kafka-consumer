#!/usr/bin/env bash

set -uo pipefail

runtime_dir="${KAFKA_RUNTIME_DIR:-/var/run/ckc-warmup-kafka}"
ready_file="${KAFKA_READY_FILE:-${runtime_dir}/ready}"
max_age="${KAFKA_READY_MAX_AGE_SECONDS:-15}"

[[ "${max_age}" =~ ^[1-9][0-9]*$ ]] || exit 1
[[ -s "${ready_file}" ]] || exit 1
read -r reconciled_at < "${ready_file}" || exit 1
[[ "${reconciled_at}" =~ ^[0-9]+$ ]] || exit 1
(( $(date +%s) - reconciled_at <= max_age )) || exit 1

for name in broker reconciler; do
  pid_file="${runtime_dir}/${name}.pid"
  [[ -s "${pid_file}" ]] || exit 1
  read -r pid < "${pid_file}" || exit 1
  [[ "${pid}" =~ ^[1-9][0-9]*$ ]] || exit 1
  kill -0 "${pid}" 2>/dev/null || exit 1
done
