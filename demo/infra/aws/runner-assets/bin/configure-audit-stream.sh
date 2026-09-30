#!/usr/bin/env bash

set -euo pipefail

REGION="${1:?region is required}"
BUCKET="${2:?bucket is required}"
PREFIX="${3:?S3 prefix is required}"
RUN_ID="${4:?run id is required}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
CONFIG="${RUNNER_HOME}/config/fluent-bit.yaml"
AUDIT_DIR="${RUNNER_HOME}/audit"
RUN_AUDIT_DIR="${RUNNER_HOME}/reports/${RUN_ID}/audit"

for value in "${REGION}" "${BUCKET}" "${PREFIX}" "${RUN_ID}"; do
  if [[ ! "${value}" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    echo "Unsafe audit stream argument: ${value}" >&2
    exit 2
  fi
done

docker stop --time 30 audit >/dev/null 2>&1 || true
rm -rf "${AUDIT_DIR}/s3-buffer"
mkdir -p "${AUDIT_DIR}/s3-buffer" "${RUN_AUDIT_DIR}/chunks"
truncate -s 0 "${AUDIT_DIR}/audit.log"
rm -f "${RUN_AUDIT_DIR}/streamed-to-s3" "${RUN_AUDIT_DIR}/chunks/STREAM_COMPLETE.json"

cat > "${CONFIG}" <<EOF
service:
  flush: 1
  daemon: off
  log_level: info
  http_server: on
  http_listen: 0.0.0.0
  http_port: 2020
  health_check: on

pipeline:
  inputs:
    - name: tcp
      listen: 0.0.0.0
      port: 5170
      chunk_size: 256
      buffer_size: 1024
      format: json

  outputs:
    - name: file
      match: '*'
      path: /audit
      file: audit.log
      format: template
      template: '{message}'

    - name: s3
      match: '*'
      bucket: ${BUCKET}
      region: ${REGION}
      store_dir: /audit/s3-buffer
      store_dir_limit_size: 4G
      total_file_size: 128M
      upload_timeout: 1m
      use_put_object: on
      compression: gzip
      log_key: message
      preserve_data_ordering: on
      retry_limit: no_limits
      s3_key_format: '${PREFIX}/audit-%Y%m%dT%H%M%S-\$UUID.log.gz'
EOF

chown -R 1000:1000 "${AUDIT_DIR}"
docker start audit >/dev/null
sleep 2
for _ in $(seq 1 30); do
  if [[ "$(docker inspect --format '{{.State.Running}}' audit 2>/dev/null || true)" == "true" ]]; then
    echo "AWS audit streaming configured: s3://${BUCKET}/${PREFIX}/"
    exit 0
  fi
  sleep 1
done

docker logs --tail 100 audit >&2 || true
echo "Audit collector did not become ready." >&2
exit 1
