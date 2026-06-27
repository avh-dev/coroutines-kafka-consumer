#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <service=fingerprint> [...]" >&2
  exit 1
fi

image_tar="${LAB_ROOT}/state/images/ckc-lab-images.tar"
image_names=""
mkdir -p "${LAB_ROOT}/state/images" "${LAB_ROOT}/state/fingerprints/images"

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
  if [ ! -f "${LAB_ROOT}/docker/build/${service}/Dockerfile" ]; then
    echo "Dockerfile is missing for ${service}." >&2
    exit 1
  fi
  if [ ! -d "${LAB_ROOT}/docker/build/${service}/build/install/ckc-${service}" ]; then
    echo "Runtime dist is missing for ${service}." >&2
    exit 1
  fi

  docker build -t "ckc-perf/${service}:latest" "${LAB_ROOT}/docker/build/${service}"
  image_names="${image_names} ckc-perf/${service}:latest"
done

# shellcheck disable=SC2086
docker save ${image_names} -o "${image_tar}"
k3s ctr images import "${image_tar}"
rm -f "${image_tar}"

for request in "$@"; do
  service="${request%%=*}"
  fingerprint="${request#*=}"
  printf '%s\n' "${fingerprint}" > "${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint"
done

docker image ls 'ckc-perf/*'
k3s ctr images list | grep -E 'ckc-perf/(demo|demo-stubs)'
echo "Requested lab images were rebuilt and loaded into k3s."
