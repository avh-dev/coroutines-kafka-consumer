#!/usr/bin/env bash

set -euo pipefail

REGION="${1:?region is required}"
BUCKET="${2:?bucket is required}"
PREFIX="${3:?S3 prefix is required}"
RUN_ID="${4:?run id is required}"
RUNNER_HOME="${CKC_RUNNER_HOME:-/opt/ckc-runner}"
RUN_AUDIT_DIR="${RUNNER_HOME}/reports/${RUN_ID}/audit"
INVENTORY="$(mktemp)"
MARKER="${RUN_AUDIT_DIR}/stream-complete.json"

cleanup() {
  rm -f "${INVENTORY}"
}
trap cleanup EXIT

# A graceful stop makes the S3 output upload its last partial PutObject chunk.
docker stop --time 120 audit >/dev/null
if find "${RUNNER_HOME}/audit/s3-buffer" -type f -size +0c -print -quit | grep -q .; then
  echo "Fluent Bit left non-empty audit data in its S3 buffer after shutdown." >&2
  find "${RUNNER_HOME}/audit/s3-buffer" -type f -size +0c -printf '%p %s bytes\n' >&2
  exit 1
fi
mkdir -p "$(dirname "${MARKER}")"
aws s3api list-objects-v2 \
  --region "${REGION}" \
  --bucket "${BUCKET}" \
  --prefix "${PREFIX}/" \
  --output json > "${INVENTORY}"

python3 - "${INVENTORY}" "${MARKER}" "${PREFIX}" <<'PY'
import datetime
import json
import pathlib
import sys

inventory_path, marker_path, prefix = sys.argv[1:]
inventory = json.loads(pathlib.Path(inventory_path).read_text(encoding="utf-8"))
chunks = []
for item in inventory.get("Contents", []):
    key = str(item.get("Key", ""))
    if not key.endswith(".log.gz"):
        continue
    chunks.append({
        "name": pathlib.PurePosixPath(key).name,
        "size": int(item["Size"]),
        "etag": str(item.get("ETag", "")).strip('"'),
    })
if not chunks:
    raise SystemExit("No streamed audit chunks were found in S3")
marker = {
    "schema_version": 1,
    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "s3_prefix": prefix,
    "chunks": sorted(chunks, key=lambda item: item["name"]),
}
path = pathlib.Path(marker_path)
path.write_text(json.dumps(marker, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

aws s3 cp "${MARKER}" "s3://${BUCKET}/${PREFIX}/STREAM_COMPLETE.json" \
  --region "${REGION}" --only-show-errors
echo "AWS audit stream completed: s3://${BUCKET}/${PREFIX}/"
