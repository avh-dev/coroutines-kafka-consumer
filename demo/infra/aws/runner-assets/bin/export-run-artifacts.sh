#!/usr/bin/env bash

set -euo pipefail

REGION="${1:?region is required}"
ENVIRONMENT="${2:?environment is required}"
RUN_ID="${3:?run id is required}"
BUCKET="${4:?artifact bucket is required}"
PREFIX="${5:-sessions/${RUN_ID}/result}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
REPO_DIR="${CKC_RUNNER_REPO_DIR:-${RUNNER_HOME}/assets/repo}"
RUN_DIR="${RUNNER_HOME}/reports/${RUN_ID}"
AUDIT_SOURCE="${RUNNER_HOME}/audit/audit.log"

if [ ! -d "${RUN_DIR}" ]; then
  echo "Run result directory was not found: ${RUN_DIR}" >&2
  exit 1
fi

mkdir -p "${RUN_DIR}/audit/chunks" "${RUN_DIR}/metrics" "${RUN_DIR}/logs/runner" "${RUN_DIR}/config"

python3 "${REPO_DIR}/demo/infra/shared/result_bundle/export-loki.py" \
  "${RUN_DIR}" --loki-url http://127.0.0.1:3100

sleep 2
if [ -f "${AUDIT_SOURCE}" ] && ! find "${RUN_DIR}/audit/chunks" -maxdepth 1 -type f -name '*.log.gz' | grep -q .; then
  gzip -c "${AUDIT_SOURCE}" > "${RUN_DIR}/audit/chunks/audit-000001.log.gz"
fi

for container in prometheus loki grafana audit ckc-msk-cloudwatch-exporter ckc-msk-cloudwatch-vmagent; do
  docker logs "${container}" > "${RUN_DIR}/logs/runner/${container}.log" 2>&1 || true
done

cp "${RUNNER_HOME}/config/load-lab-${ENVIRONMENT}.json" "${RUN_DIR}/config/" 2>/dev/null || true
cp "${RUNNER_HOME}/observability/grafana/dashboards/ckc-overview.json" "${RUN_DIR}/config/" 2>/dev/null || true
cp "${RUNNER_HOME}/reports/session-${RUN_ID}.log" "${RUN_DIR}/logs/runner/session.log" 2>/dev/null || true

if docker inspect prometheus >/dev/null 2>&1; then
  docker stop --time 30 prometheus >/dev/null
  tar -C "${RUNNER_HOME}" -czf "${RUN_DIR}/metrics/victoriametrics-data.tar.gz" prometheus
fi

"${REPO_DIR}/demo/infra/aws/restore/package-result.sh" "${RUN_DIR}"
python3 "${REPO_DIR}/demo/infra/aws/runner-assets/bin/build-artifact-manifest.py" \
  "${RUN_DIR}" --run-id "${RUN_ID}"

aws s3 sync "${RUN_DIR}/" "s3://${BUCKET}/${PREFIX}/" --region "${REGION}" --only-show-errors
printf 'complete\n' > "${RUN_DIR}/COMPLETE"
aws s3 cp "${RUN_DIR}/COMPLETE" "s3://${BUCKET}/${PREFIX}/COMPLETE" --region "${REGION}" --only-show-errors

echo "Artifacts uploaded."
echo "  run_id=${RUN_ID}"
echo "  s3_uri=s3://${BUCKET}/${PREFIX}/"
