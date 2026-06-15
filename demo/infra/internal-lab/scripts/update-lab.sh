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

fingerprint_paths() {
  local label="$1"
  shift

  if ! command -v sha256sum >/dev/null 2>&1; then
    echo "sha256sum is required to calculate internal-lab fingerprints." >&2
    exit 1
  fi

  (
    cd "${REPO_ROOT}"
    {
      printf '%s\n' "internal-lab-fingerprint-v1-${label}"
      for path in "$@"; do
        if [[ -f "${path}" ]]; then
          sha256sum "${path}"
        else
          find "${path}" \
            -type f \
            ! -path '*/build/*' \
            ! -path '*/.gradle/*' \
            -print0 \
            | sort -z \
            | xargs -0 sha256sum
        fi
      done
    } | sha256sum | awk '{ print $1 }'
  )
}

image_fingerprint() {
  local service="$1"
  local -a paths=(
    settings.gradle.kts
    build.gradle.kts
    gradle.properties
    gradle/wrapper/gradle-wrapper.properties
  )

  case "${service}" in
    demo)
      paths+=(ckc-core ckc-micrometer demo/ckc-demo-contracts demo/ckc-demo)
      ;;
    demo-stubs)
      paths+=(demo/ckc-demo-stubs)
      ;;
    *)
      echo "Unknown image service: ${service}" >&2
      exit 1
      ;;
  esac

  fingerprint_paths "image-v3-${service}" "${paths[@]}"
}

remote_image_is_current() {
  local service="$1"
  local fingerprint="$2"

  ssh "root@${LAB_HOST}" \
    "test \"\$(cat '${LAB_ROOT}/images/${service}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}' && k3s ctr images list -q | grep -Fxq 'docker.io/ckc-perf/${service}:latest'"
}

remote_fingerprint_matches() {
  local name="$1"
  local fingerprint="$2"

  ssh "root@${LAB_HOST}" \
    "test \"\$(cat '${LAB_ROOT}/fingerprints/${name}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}'"
}

record_remote_fingerprint() {
  local name="$1"
  local fingerprint="$2"

  ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/fingerprints' && printf '%s\n' '${fingerprint}' > '${LAB_ROOT}/fingerprints/${name}.fingerprint'"
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

ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/config' '${LAB_ROOT}/workspace' '${LAB_ROOT}/build-context/demo/build/install' '${LAB_ROOT}/build-context/demo-stubs/build/install' '${LAB_ROOT}/runtime/load-test' '${LAB_ROOT}/images' '${LAB_ROOT}/fingerprints'"
ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
EOF

DEMO_FINGERPRINT="$(image_fingerprint demo)"
DEMO_STUBS_FINGERPRINT="$(image_fingerprint demo-stubs)"
LOAD_TEST_RUNTIME_FINGERPRINT="$(fingerprint_paths "load-test-runtime" \
  settings.gradle.kts \
  build.gradle.kts \
  gradle.properties \
  gradle/wrapper/gradle-wrapper.properties \
  demo/ckc-demo-contracts \
  demo/ckc-demo-load-test)"
ASSETS_SYNC_FINGERPRINT="$(fingerprint_paths "assets-sync" demo/infra/internal-lab/assets)"
SHARED_SYNC_FINGERPRINT="$(fingerprint_paths "shared-sync" demo/infra/shared)"
INTERNAL_LAB_HELM_SYNC_FINGERPRINT="$(fingerprint_paths "internal-lab-helm-sync" demo/infra/internal-lab/helm)"
BASE_DEPLOY_FINGERPRINT="$(fingerprint_paths "base-deploy" \
  demo/infra/internal-lab/assets/compose \
  demo/infra/internal-lab/assets/grafana \
  demo/infra/internal-lab/assets/k8s \
  demo/infra/internal-lab/assets/libexec/deploy-base.sh \
  demo/infra/shared/grafana \
  demo/infra/internal-lab/helm/demo)"
STUBS_DEPLOY_FINGERPRINT="$(fingerprint_paths "stubs-deploy" \
  demo/infra/internal-lab/assets/config/demo-stubs-values.yaml \
  demo/infra/internal-lab/assets/libexec/deploy-stubs.sh \
  demo/infra/internal-lab/helm/demo-stubs)"

DEMO_IMAGE_CHANGED=0
DEMO_STUBS_IMAGE_CHANGED=0
LOAD_TEST_RUNTIME_CHANGED=0
ASSETS_SYNC_CHANGED=0
SHARED_SYNC_CHANGED=0
INTERNAL_LAB_HELM_SYNC_CHANGED=0
BASE_DEPLOY_CHANGED=0
STUBS_DEPLOY_CHANGED=0

if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_image_is_current demo "${DEMO_FINGERPRINT}"; then
  DEMO_IMAGE_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_image_is_current demo-stubs "${DEMO_STUBS_FINGERPRINT}"; then
  DEMO_STUBS_IMAGE_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "load-test-runtime" "${LOAD_TEST_RUNTIME_FINGERPRINT}"; then
  LOAD_TEST_RUNTIME_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "assets-sync" "${ASSETS_SYNC_FINGERPRINT}"; then
  ASSETS_SYNC_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "shared-sync" "${SHARED_SYNC_FINGERPRINT}"; then
  SHARED_SYNC_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "internal-lab-helm-sync" "${INTERNAL_LAB_HELM_SYNC_FINGERPRINT}"; then
  INTERNAL_LAB_HELM_SYNC_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "base-deploy" "${BASE_DEPLOY_FINGERPRINT}"; then
  BASE_DEPLOY_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "stubs-deploy" "${STUBS_DEPLOY_FINGERPRINT}"; then
  STUBS_DEPLOY_CHANGED=1
fi

cd "${REPO_ROOT}"
GRADLE_TASKS=()
if [[ "${LOAD_TEST_RUNTIME_CHANGED}" -eq 1 ]]; then
  GRADLE_TASKS+=(:ckc-demo-load-test:installDist)
fi
if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  GRADLE_TASKS+=(:ckc-demo:installDist)
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  GRADLE_TASKS+=(:ckc-demo-stubs:installDist)
fi
if [[ "${#GRADLE_TASKS[@]}" -gt 0 ]]; then
  ./gradlew "${GRADLE_TASKS[@]}"
fi

if [[ "${ASSETS_SYNC_CHANGED}" -eq 1 ]]; then
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets" "${LAB_ROOT}/assets"
  ssh "root@${LAB_HOST}" "cp '${LAB_ROOT}/assets/compose/docker-compose.host-services.yml' '${LAB_ROOT}/docker-compose.host-services.yml'"
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/assets/bin/'*.sh '${LAB_ROOT}/assets/libexec/'*.sh"
  record_remote_fingerprint "assets-sync" "${ASSETS_SYNC_FINGERPRINT}"
fi

if [[ "${SHARED_SYNC_CHANGED}" -eq 1 ]]; then
  sync_path "${REPO_ROOT}/demo/infra/shared" "${LAB_ROOT}/workspace/demo/infra/shared"
  record_remote_fingerprint "shared-sync" "${SHARED_SYNC_FINGERPRINT}"
fi
if [[ "${INTERNAL_LAB_HELM_SYNC_CHANGED}" -eq 1 ]]; then
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/helm" "${LAB_ROOT}/workspace/demo/infra/internal-lab/helm"
  record_remote_fingerprint "internal-lab-helm-sync" "${INTERNAL_LAB_HELM_SYNC_FINGERPRINT}"
fi

if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  sync_file "${REPO_ROOT}/demo/ckc-demo/Dockerfile" "${LAB_ROOT}/build-context/demo/Dockerfile"
  sync_path "${REPO_ROOT}/demo/ckc-demo/build/install/ckc-demo" "${LAB_ROOT}/build-context/demo/build/install/ckc-demo"
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  sync_file "${REPO_ROOT}/demo/ckc-demo-stubs/Dockerfile" "${LAB_ROOT}/build-context/demo-stubs/Dockerfile"
  sync_path "${REPO_ROOT}/demo/ckc-demo-stubs/build/install/ckc-demo-stubs" "${LAB_ROOT}/build-context/demo-stubs/build/install/ckc-demo-stubs"
fi
if [[ "${LOAD_TEST_RUNTIME_CHANGED}" -eq 1 ]]; then
  sync_path "${REPO_ROOT}/demo/ckc-demo-load-test/build/install/ckc-demo-load-test" "${LAB_ROOT}/runtime/load-test"
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/runtime/load-test/bin/'*"
  record_remote_fingerprint "load-test-runtime" "${LOAD_TEST_RUNTIME_FINGERPRINT}"
fi

if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/build-context/demo/build/install/ckc-demo/bin/'*"
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/build-context/demo-stubs/build/install/ckc-demo-stubs/bin/'*"
fi
if [[ "${BASE_DEPLOY_CHANGED}" -eq 1 ]]; then
  ssh "root@${LAB_HOST}" "LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' LAB_ROOT='${LAB_ROOT}' ASSETS_DIR='${LAB_ROOT}/assets' '${LAB_ROOT}/assets/libexec/deploy-base.sh'"
  record_remote_fingerprint "base-deploy" "${BASE_DEPLOY_FINGERPRINT}"
fi

REBUILD_ARGS=()
if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  REBUILD_ARGS+=("demo=${DEMO_FINGERPRINT}")
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  REBUILD_ARGS+=("demo-stubs=${DEMO_STUBS_FINGERPRINT}")
fi
if [[ "${#REBUILD_ARGS[@]}" -gt 0 ]]; then
  ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/libexec/rebuild-images.sh' ${REBUILD_ARGS[*]}"
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]] || [[ "${STUBS_DEPLOY_CHANGED}" -eq 1 ]]; then
  if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
    ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/libexec/deploy-stubs.sh' --restart"
  else
    ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/assets/libexec/deploy-stubs.sh'"
  fi
  record_remote_fingerprint "stubs-deploy" "${STUBS_DEPLOY_FINGERPRINT}"
fi

echo "Internal lab is updated."
echo "  demo image changed=${DEMO_IMAGE_CHANGED}"
echo "  demo-stubs image changed=${DEMO_STUBS_IMAGE_CHANGED}"
echo "  load-test runtime changed=${LOAD_TEST_RUNTIME_CHANGED}"
echo "  assets synced=${ASSETS_SYNC_CHANGED}"
echo "  shared synced=${SHARED_SYNC_CHANGED}"
echo "  internal-lab helm synced=${INTERNAL_LAB_HELM_SYNC_CHANGED}"
echo "  base redeployed=${BASE_DEPLOY_CHANGED}"
echo "  demo-stubs redeployed=$(( DEMO_STUBS_IMAGE_CHANGED || STUBS_DEPLOY_CHANGED ))"
echo "  runtime=${LAB_ROOT}/runtime/load-test"
echo "  lab entrypoints=${LAB_ROOT}/assets/bin"
