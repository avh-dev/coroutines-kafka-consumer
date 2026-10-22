#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <service=fingerprint> [...]" >&2
  exit 1
fi

image_tar="${LAB_ROOT}/state/images/ckc-lab-images.tar"
all_image_names="ckc-perf/demo:latest ckc-perf/demo-stubs:latest ckc-perf/load-test:latest ckc-perf/kafka-network-exporter:latest"
mkdir -p "${LAB_ROOT}/state/images" "${LAB_ROOT}/state/fingerprints/images"

for request in "$@"; do
  service="${request%%=*}"
  fingerprint="${request#*=}"
  runtime_name="ckc-${service}"

  case "${service}" in
    demo|demo-stubs|load-test|kafka-network-exporter)
      ;;
    *)
      echo "Unknown image service: ${service}" >&2
      exit 1
      ;;
  esac
  if [ "${service}" = "load-test" ]; then
    runtime_name="ckc-demo-load-test"
  fi

  if [ -z "${fingerprint}" ] || [ "${fingerprint}" = "${request}" ]; then
    echo "Fingerprint is missing for ${service}." >&2
    exit 1
  fi
  if [ ! -f "${LAB_ROOT}/docker/build/${service}/Dockerfile" ]; then
    echo "Dockerfile is missing for ${service}." >&2
    exit 1
  fi
  if [ "${service}" != "kafka-network-exporter" ] && [ ! -d "${LAB_ROOT}/docker/build/${service}/build/install/${runtime_name}" ]; then
    echo "Runtime dist is missing for ${service}." >&2
    exit 1
  fi

  docker build -t "ckc-perf/${service}:latest" "${LAB_ROOT}/docker/build/${service}"
done

# Import the complete local image set even when only one image was rebuilt. A
# partial archive is not a safe recovery source if the k3s/containerd image
# store was reset or garbage-collected between lab updates.
for image_name in ${all_image_names}; do
  if ! docker image inspect "${image_name}" >/dev/null 2>&1; then
    echo "Required lab image is missing from Docker: ${image_name}" >&2
    echo "Run a full lab image rebuild before retrying the update." >&2
    exit 1
  fi
done

# shellcheck disable=SC2086
docker save ${all_image_names} -o "${image_tar}"
sudo -n /usr/local/libexec/ckc-lab/import-k3s-images
if [ -n "${LAB_APPLICATION_TARGET:-}" ]; then
  ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
    "${LAB_APPLICATION_TARGET}" "mkdir -p '${LAB_ROOT}/state/images'"
  scp -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
    "${image_tar}" "${LAB_APPLICATION_TARGET}:${image_tar}"
  ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
    "${LAB_APPLICATION_TARGET}" \
    "sudo -n /usr/local/libexec/ckc-lab/import-k3s-images && rm -f -- '${image_tar}'"
fi
rm -f "${image_tar}"

for request in "$@"; do
  service="${request%%=*}"
  fingerprint="${request#*=}"
  printf '%s\n' "${fingerprint}" > "${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint"
  if [ -n "${LAB_APPLICATION_TARGET:-}" ]; then
    ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
      "${LAB_APPLICATION_TARGET}" \
      "mkdir -p '${LAB_ROOT}/state/fingerprints/images' && printf '%s\\n' '${fingerprint}' > '${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint'"
  fi
done

docker image ls 'ckc-perf/*'
echo "Requested lab images were rebuilt and loaded into k3s."
