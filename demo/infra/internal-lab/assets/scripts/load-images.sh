#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
IMAGE_ARCHIVE="${1:-${LAB_ROOT}/images/ckc-internal-lab-images.tar.gz}"

if [ ! -f "${IMAGE_ARCHIVE}" ]; then
  echo "Image archive was not found: ${IMAGE_ARCHIVE}" >&2
  exit 1
fi

gzip -dc "${IMAGE_ARCHIVE}" | docker load
gzip -dc "${IMAGE_ARCHIVE}" | k3s ctr images import -

docker image ls 'ckc-perf/*'
k3s ctr images list | grep -E 'ckc-perf/(demo|demo-stubs)'
