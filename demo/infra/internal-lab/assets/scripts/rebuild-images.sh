#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <service=fingerprint> [...]" >&2
  exit 1
fi

image_tar="${LAB_ROOT}/images/ckc-internal-lab-images.tar"
image_names=""

for request in "$@"; do
  service="${request%%=*}"
  fingerprint="${request#*=}"

  case "${service}" in
    demo|demo-stubs)
      ;;
    *)
      echo "Unknown image service: ${service}" >&2
      exit 1
      ;;
  esac

  if [ -z "${fingerprint}" ] || [ "${fingerprint}" = "${request}" ]; then
    echo "Fingerprint is missing for ${service}." >&2
    exit 1
  fi
  if [ ! -f "${LAB_ROOT}/build-context/${service}/Dockerfile" ]; then
    echo "Dockerfile is missing for ${service}." >&2
    exit 1
  fi
  if [ ! -d "${LAB_ROOT}/build-context/${service}/build/install/ckc-${service}" ]; then
    echo "Runtime dist is missing for ${service}." >&2
    exit 1
  fi

  docker build -t "ckc-perf/${service}:latest" "${LAB_ROOT}/build-context/${service}"
  image_names="${image_names} ckc-perf/${service}:latest"
done

# shellcheck disable=SC2086
docker save ${image_names} -o "${image_tar}"
k3s ctr images import "${image_tar}"
rm -f "${image_tar}"

for request in "$@"; do
  service="${request%%=*}"
  fingerprint="${request#*=}"
  printf '%s\n' "${fingerprint}" > "${LAB_ROOT}/images/${service}.fingerprint"
done

docker image ls 'ckc-perf/*'
k3s ctr images list | grep -E 'ckc-perf/(demo|demo-stubs)'
echo "Requested lab images were rebuilt and loaded into k3s."
