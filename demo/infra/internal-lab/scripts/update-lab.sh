#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
FORCE_REBUILD=0

usage() {
  cat <<EOF
Usage: $0 [--force-rebuild]

Builds local JVM runtime distributions, syncs them to the internal lab host,
and rebuilds/reloads lab Docker images on the lab host when needed.

Options:
  --force-rebuild  Rebuild and reload lab images even if the fingerprint matches.
  -h, --help       Show this help.
EOF
}

resolve_host_ip() {
  local host="$1"

  if command -v getent >/dev/null 2>&1; then
    getent ahostsv4 "${host}" | awk 'NR == 1 { print $1 }'
    return
  fi

  python - "${host}" <<'PY'
import socket
import sys

print(socket.gethostbyname(sys.argv[1]))
PY
}

image_fingerprint() {
  if ! command -v sha256sum >/dev/null 2>&1; then
    echo "sha256sum is required to calculate the lab image fingerprint." >&2
    exit 1
  fi

  (
    cd "${REPO_ROOT}"
    {
      printf '%s\n' "internal-lab-image-fingerprint-v2"
      for path in \
        settings.gradle.kts \
        build.gradle.kts \
        gradle.properties \
        gradle/wrapper/gradle-wrapper.properties \
        ckc-core \
        ckc-micrometer \
        demo/ckc-demo-contracts \
        demo/ckc-demo \
        demo/ckc-demo-stubs
      do
        if [[ -f "${path}" ]]; then
          sha256sum "${path}"
        elif [[ -d "${path}" ]]; then
          find "${path}" \
            -type f \
            ! -path '*/build/*' \
            ! -path '*/.gradle/*' \
            -print0 \
            | sort -z \
            | xargs -0 sha256sum
        fi
      done
      for path in \
        demo/ckc-demo/build/install/ckc-demo \
        demo/ckc-demo-stubs/build/install/ckc-demo-stubs
      do
        if [[ -d "${path}" ]]; then
          find "${path}" -type f -print0 | sort -z | xargs -0 sha256sum
        fi
      done
    } | sha256sum | awk '{ print $1 }'
  )
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force-rebuild)
      FORCE_REBUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "${STATE_DIR}/lab.env" ]]; then
  echo "Lab state was not found. Run demo/infra/internal-lab/scripts/install-lab.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${STATE_DIR}/lab.env"

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
LAB_HOST="${LAB_HOST:-}"
if [[ -z "${LAB_HOST:-}" ]]; then
  echo "LAB_HOST is missing from ${STATE_DIR}/lab.env. Run demo/infra/internal-lab/scripts/install-lab.sh first." >&2
  exit 1
fi
LAB_NODE_IP="$(resolve_host_ip "${LAB_HOST}")"
if [[ -z "${LAB_NODE_IP}" ]]; then
  echo "Unable to resolve lab host: ${LAB_HOST}" >&2
  exit 1
fi
sync_path() {
  local source_path="$1"
  local target_path="$2"

  ssh "root@${LAB_HOST}" "mkdir -p '$(printf "%q" "${target_path}")'"

  if command -v rsync >/dev/null 2>&1; then
    rsync -az --delete "${source_path%/}/" "root@${LAB_HOST}:${target_path%/}/"
    return
  fi

  echo "rsync was not found; using tar over ssh for ${source_path}."
  ssh "root@${LAB_HOST}" "rm -rf '$(printf "%q" "${target_path}")' && mkdir -p '$(printf "%q" "${target_path}")'"
  tar -C "${source_path}" -cf - . | ssh "root@${LAB_HOST}" "tar -C '${target_path}' -xf -"
}

sync_file() {
  local source_path="$1"
  local target_path="$2"
  local target_dir

  target_dir="$(dirname "${target_path}")"
  ssh "root@${LAB_HOST}" "mkdir -p '$(printf "%q" "${target_dir}")'"
  scp "${source_path}" "root@${LAB_HOST}:${target_path}"
}

cd "${REPO_ROOT}"
./gradlew :ckc-demo:installDist :ckc-demo-stubs:installDist :ckc-demo-load-test:installDist

IMAGE_FINGERPRINT="$(image_fingerprint)"

ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config' '${LAB_ROOT}/workspace' '${LAB_ROOT}/build-context/demo/build/install' '${LAB_ROOT}/build-context/demo-stubs/build/install' '${LAB_ROOT}/runtime/load-test' '${LAB_ROOT}/images'"
ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
EOF

sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets" "${LAB_ROOT}/assets"
sync_path "${REPO_ROOT}/demo/infra/shared" "${LAB_ROOT}/workspace/demo/infra/shared"
ssh "root@${LAB_HOST}" "cp '${LAB_ROOT}/assets/compose/docker-compose.host-services.yml' '${LAB_ROOT}/docker-compose.host-services.yml'"

sync_file "${REPO_ROOT}/demo/ckc-demo/Dockerfile" "${LAB_ROOT}/build-context/demo/Dockerfile"
sync_file "${REPO_ROOT}/demo/ckc-demo-stubs/Dockerfile" "${LAB_ROOT}/build-context/demo-stubs/Dockerfile"
sync_path "${REPO_ROOT}/demo/ckc-demo/build/install/ckc-demo" "${LAB_ROOT}/build-context/demo/build/install/ckc-demo"
sync_path "${REPO_ROOT}/demo/ckc-demo-stubs/build/install/ckc-demo-stubs" "${LAB_ROOT}/build-context/demo-stubs/build/install/ckc-demo-stubs"
sync_path "${REPO_ROOT}/demo/ckc-demo-load-test/build/install/ckc-demo-load-test" "${LAB_ROOT}/runtime/load-test"
printf '%s\n' "${IMAGE_FINGERPRINT}" | ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/images/images.fingerprint.next'"

ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/assets/scripts/'*.sh '${LAB_ROOT}/build-context/demo/build/install/ckc-demo/bin/'* '${LAB_ROOT}/build-context/demo-stubs/build/install/ckc-demo-stubs/bin/'* '${LAB_ROOT}/runtime/load-test/bin/'*"
ssh "root@${LAB_HOST}" "LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' docker compose -f '${LAB_ROOT}/docker-compose.host-services.yml' up -d --no-deps grafana"
ssh "root@${LAB_HOST}" "docker compose -f '${LAB_ROOT}/docker-compose.host-services.yml' restart grafana"

REBUILD_ARGS=()
if [[ "${FORCE_REBUILD}" -eq 1 ]]; then
  REBUILD_ARGS+=(--force)
fi

ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/scripts/rebuild-images.sh' '${IMAGE_FINGERPRINT}' ${REBUILD_ARGS[*]}"
ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/scripts/deploy-stubs.sh' --restart"

echo "Internal lab is updated."
echo "  fingerprint=${IMAGE_FINGERPRINT}"
echo "  runtime=${LAB_ROOT}/runtime/load-test"
echo "  lab scripts=${LAB_ROOT}/assets/scripts"
