#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
REQUESTED_FINGERPRINT="${1:-}"
FORCE_REBUILD=0

if [ "${2:-}" = "--force" ]; then
  FORCE_REBUILD=1
fi

if [ -z "${REQUESTED_FINGERPRINT}" ]; then
  echo "Usage: $0 <fingerprint> [--force]" >&2
  exit 1
fi

current_fingerprint="$(cat "${LAB_ROOT}/images/images.fingerprint" 2>/dev/null || true)"

images_present() {
  k3s ctr images list -q | grep -Fxq 'docker.io/ckc-perf/demo:latest' \
    && k3s ctr images list -q | grep -Fxq 'docker.io/ckc-perf/demo-stubs:latest'
}

if [ "${FORCE_REBUILD}" -eq 0 ] && [ "${current_fingerprint}" = "${REQUESTED_FINGERPRINT}" ] && images_present; then
  echo "Lab images are current."
  echo "  fingerprint=${REQUESTED_FINGERPRINT}"
  exit 0
fi

for service in demo demo-stubs; do
  if [ ! -f "${LAB_ROOT}/build-context/${service}/Dockerfile" ]; then
    echo "Dockerfile is missing for ${service}." >&2
    exit 1
  fi
  if [ ! -d "${LAB_ROOT}/build-context/${service}/build/install/ckc-${service}" ]; then
    echo "Runtime dist is missing for ${service}." >&2
    exit 1
  fi
done

docker build -t ckc-perf/demo:latest "${LAB_ROOT}/build-context/demo"
docker build -t ckc-perf/demo-stubs:latest "${LAB_ROOT}/build-context/demo-stubs"

image_tar="${LAB_ROOT}/images/ckc-internal-lab-images.tar"
docker save ckc-perf/demo:latest ckc-perf/demo-stubs:latest -o "${image_tar}"
k3s ctr images import "${image_tar}"
rm -f "${image_tar}"

printf '%s\n' "${REQUESTED_FINGERPRINT}" > "${LAB_ROOT}/images/images.fingerprint"

docker image ls 'ckc-perf/*'
k3s ctr images list | grep -E 'ckc-perf/(demo|demo-stubs)'
echo "Lab images were rebuilt and loaded into k3s."
echo "  fingerprint=${REQUESTED_FINGERPRINT}"
