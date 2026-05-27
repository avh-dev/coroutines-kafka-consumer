#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/image-fingerprint-lib.sh"

if [[ ! -f "${STATE_DIR}/lab.env" ]]; then
  echo "Lab state was not found. Run demo/infra/internal-lab/scripts/install-lab.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${STATE_DIR}/lab.env"

ARCHIVE="${STATE_DIR}/ckc-internal-lab-images.tar.gz"
FINGERPRINT_PATH="${STATE_DIR}/images.fingerprint"
IMAGE_FINGERPRINT="$(image_fingerprint)"

cd "${REPO_ROOT}"
./gradlew :ckc-demo:bootJar :ckc-demo-stubs:fatJar

if ! docker info >/dev/null 2>&1; then
  echo "Local Docker is not reachable. Start Docker Desktop or the local Docker daemon, then rerun this script." >&2
  exit 1
fi

docker build -f demo/ckc-demo/Dockerfile -t ckc-perf/demo:latest demo/ckc-demo
docker build -f demo/ckc-demo-stubs/Dockerfile -t ckc-perf/demo-stubs:latest demo/ckc-demo-stubs
docker save ckc-perf/demo:latest ckc-perf/demo-stubs:latest | gzip > "${ARCHIVE}"
printf '%s\n' "${IMAGE_FINGERPRINT}" > "${FINGERPRINT_PATH}"

ssh "${SSH_TARGET}" "mkdir -p '${LAB_ROOT}/images'"
scp "${ARCHIVE}" "${SSH_TARGET}:${LAB_ROOT}/images/ckc-internal-lab-images.tar.gz"
ssh "${SSH_TARGET}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/scripts/load-images.sh' '${LAB_ROOT}/images/ckc-internal-lab-images.tar.gz'"
scp "${FINGERPRINT_PATH}" "${SSH_TARGET}:${LAB_ROOT}/images/images.fingerprint"

echo "Images are built locally and loaded into the lab."
echo "  fingerprint=${IMAGE_FINGERPRINT}"
