#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
WORK_DIR="${CKC_AWS_AUDIT_WORK_DIR:-${REPO_ROOT}/.demo-infra/aws/audit}"
UPLOAD_SUMMARY=0
METADATA_FILE=""
S3_URI=""

usage() {
  cat <<EOF
Usage: $0 [--metadata-file path] [--upload-summary] s3://bucket/prefix/run-id

Downloads AWS audit chunks from S3, runs the shared audit analyzer locally, and
writes summary.yaml plus analyzer-progress.log under .demo-infra/aws/audit.
The S3 prefix is expected to contain chunks/*.log.gz and may contain
run-metadata.json.
EOF
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --metadata-file)
      METADATA_FILE="${2:?--metadata-file requires a path}"
      shift 2
      ;;
    --upload-summary)
      UPLOAD_SUMMARY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -n "${S3_URI}" ]]; then
        echo "Only one S3 URI can be provided." >&2
        usage >&2
        exit 1
      fi
      S3_URI="$1"
      shift
      ;;
  esac
done

if [[ -z "${S3_URI}" ]]; then
  echo "S3 URI is required." >&2
  usage >&2
  exit 1
fi
if [[ "${S3_URI}" != s3://* ]]; then
  echo "S3 URI must start with s3://: ${S3_URI}" >&2
  exit 1
fi

RUN_ID="$(basename "${S3_URI%/}")"
RUN_DIR="${WORK_DIR}/${RUN_ID}"
SUMMARY_FILE="${RUN_DIR}/summary.yaml"
PROGRESS_FILE="${RUN_DIR}/analyzer-progress.log"
CHUNKS_DIR="${RUN_DIR}/chunks"

rm -rf "${RUN_DIR}"
mkdir -p "${RUN_DIR}"

aws s3 sync "${S3_URI%/}/" "${RUN_DIR}/"

if [[ -z "${METADATA_FILE}" && -f "${RUN_DIR}/run-metadata.json" ]]; then
  METADATA_FILE="${RUN_DIR}/run-metadata.json"
fi

ANALYZE_ARGS=(
  python3
  "${REPO_ROOT}/demo/infra/shared/audit/analyze-audit.py"
  --input-dir
  "${CHUNKS_DIR}"
  --require-records
)
if [[ -n "${METADATA_FILE}" ]]; then
  ANALYZE_ARGS+=(--metadata-file "${METADATA_FILE}")
fi

if ! "${ANALYZE_ARGS[@]}" > "${SUMMARY_FILE}" 2> >(tee "${PROGRESS_FILE}" >&2); then
  echo "Audit analysis failed. Progress log: ${PROGRESS_FILE}" >&2
  exit 1
fi

if [[ "${UPLOAD_SUMMARY}" -eq 1 ]]; then
  aws s3 cp "${SUMMARY_FILE}" "${S3_URI%/}/summary.yaml"
  aws s3 cp "${PROGRESS_FILE}" "${S3_URI%/}/analyzer-progress.log"
fi

cat "${SUMMARY_FILE}"
echo "summary=${SUMMARY_FILE}" >&2
