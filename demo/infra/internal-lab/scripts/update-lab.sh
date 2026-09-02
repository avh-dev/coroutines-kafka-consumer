#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
DEFAULT_LAB_ROOT="/opt/ckc-lab"
LEGACY_LAB_ROOT="/opt/ckc-internal-lab"
DEFAULT_THREAD_STATS_REPO="${REPO_ROOT}/../thread-stats"
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
            ! -path '*/__pycache__/*' \
            ! -name '*.pyc' \
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
    "test \"\$(cat '${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}' && k3s ctr images list -q | grep -Fxq 'docker.io/ckc-perf/${service}:latest'"
}

remote_fingerprint_matches() {
  local name="$1"
  local fingerprint="$2"

  ssh "root@${LAB_HOST}" \
    "test \"\$(cat '${LAB_ROOT}/state/fingerprints/${name}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}'"
}

remote_paths_exist() {
  local -a paths=("$@")
  local command="set -e;"
  local path

  for path in "${paths[@]}"; do
    command="${command} test -e '$(printf "%q" "${path}")';"
  done
  ssh "root@${LAB_HOST}" "${command}"
}

build_thread_stats_agent() {
  if [[ -n "${THREAD_STATS_AGENT_JAR:-}" ]]; then
    if [[ ! -f "${THREAD_STATS_AGENT_JAR}" ]]; then
      echo "THREAD_STATS_AGENT_JAR does not exist: ${THREAD_STATS_AGENT_JAR}" >&2
      exit 1
    fi
    printf "%s\n" "${THREAD_STATS_AGENT_JAR}"
    return
  fi

  local thread_stats_repo="${THREAD_STATS_REPO:-${DEFAULT_THREAD_STATS_REPO}}"
  if [[ ! -f "${thread_stats_repo}/thread-stats-agent/pom.xml" ]]; then
    echo "Thread Stats repo was not found: ${thread_stats_repo}" >&2
    echo "Set THREAD_STATS_REPO or THREAD_STATS_AGENT_JAR before running update-lab.sh." >&2
    exit 1
  fi

  (
    cd "${thread_stats_repo}"
    ./mvnw --batch-mode -pl thread-stats-agent -am package >&2
  )
  find "${thread_stats_repo}/thread-stats-agent/target" \
    -maxdepth 1 \
    -type f \
    -name 'thread-stats-agent-*.jar' \
    ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' \
    | sort \
    | tail -n 1
}

record_remote_fingerprint() {
  local name="$1"
  local fingerprint="$2"

  ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/state/fingerprints' && printf '%s\n' '${fingerprint}' > '${LAB_ROOT}/state/fingerprints/${name}.fingerprint'"
}

sync_internal_lab_assets() {
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/bin" "${LAB_ROOT}/bin"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/libexec" "${LAB_ROOT}/libexec"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/helpers" "${LAB_ROOT}/helpers"
  sync_path "${REPO_ROOT}/demo/infra/shared/helm" "${LAB_ROOT}/helm"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/compose" "${LAB_ROOT}/docker/compose"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/k8s" "${LAB_ROOT}/k8s"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/restore" "${LAB_ROOT}/restore"
  sync_file "${REPO_ROOT}/demo/infra/shared/result_bundle/restore/import-grafana-annotations.py" "${LAB_ROOT}/restore/import-grafana-annotations.py"
  ssh "root@${LAB_HOST}" "mkdir -p '${LAB_ROOT}/notify'"
  sync_file "${REPO_ROOT}/demo/infra/internal-lab/assets/notify/README.md" "${LAB_ROOT}/notify/README.md"
  sync_file "${REPO_ROOT}/demo/infra/internal-lab/assets/notify/notify-telegram.py" "${LAB_ROOT}/notify/notify-telegram.py"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/config" "${LAB_ROOT}/config/defaults"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/grafana" "${LAB_ROOT}/grafana/templates"
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/bin/'*.sh '${LAB_ROOT}/libexec/'*.sh '${LAB_ROOT}/restore/'*.sh '${LAB_ROOT}/restore/'*.py '${LAB_ROOT}/notify/'*.py 2>/dev/null || true"
}

sync_runtime_test_assets() {
  sync_path "${REPO_ROOT}/demo/infra/shared/audit" "${LAB_ROOT}/helpers/audit"
  sync_path "${REPO_ROOT}/demo/infra/shared/pcap" "${LAB_ROOT}/helpers/pcap"
  sync_path "${REPO_ROOT}/demo/infra/shared/experiment_orchestration" "${LAB_ROOT}/helpers/experiment_orchestration"
  sync_path "${REPO_ROOT}/demo/infra/shared/experiment_report" "${LAB_ROOT}/helpers/experiment_report"
  sync_path "${REPO_ROOT}/demo/infra/shared/result_bundle" "${LAB_ROOT}/helpers/result_bundle"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/workloads/experiments" "${LAB_ROOT}/workloads/experiments"
  sync_path "${REPO_ROOT}/demo/infra/shared/workloads/test-definitions" "${LAB_ROOT}/workloads/test-definitions"
  sync_path "${REPO_ROOT}/demo/infra/shared/workloads/sla-profiles" "${LAB_ROOT}/workloads/sla-profiles"
  sync_file "${REPO_ROOT}/demo/infra/shared/workloads/consumer-profiles.yaml" "${LAB_ROOT}/workloads/consumer-profiles.yaml"
  sync_path "${REPO_ROOT}/demo/infra/shared/grafana/dashboards" "${LAB_ROOT}/grafana/dashboards"
  sync_path "${REPO_ROOT}/demo/infra/shared/grafana/provisioning/dashboards" "${LAB_ROOT}/grafana/provisioning/dashboards"
  ssh "root@${LAB_HOST}" "rm -rf '${LAB_ROOT}/test-definitions' '${LAB_ROOT}/experiments' '${LAB_ROOT}/variants' '${LAB_ROOT}/test-bundles'"
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

LAB_ROOT="${LAB_ROOT:-${DEFAULT_LAB_ROOT}}"
if [[ "${LAB_ROOT}" = "${LEGACY_LAB_ROOT}" ]]; then
  LAB_ROOT="${DEFAULT_LAB_ROOT}"
fi
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
    rsync -az --delete --no-owner --no-group --exclude '__pycache__/' --exclude '*.pyc' "${source_path%/}/" "root@${LAB_HOST}:${target_path%/}/"
    ssh "root@${LAB_HOST}" "chown -R root:root '$(printf "%q" "${target_path}")'"
    return
  fi

  echo "rsync was not found; using tar over ssh for ${source_path}."
  ssh "root@${LAB_HOST}" "rm -rf '$(printf "%q" "${target_path}")' && mkdir -p '$(printf "%q" "${target_path}")'"
  tar --exclude='__pycache__' --exclude='*.pyc' -C "${source_path}" -cf - . | ssh "root@${LAB_HOST}" "tar --no-same-owner -C '${target_path}' -xf - && chown -R root:root '${target_path}'"
}

sync_file() {
  local source_path="$1"
  local target_path="$2"
  local target_dir

  target_dir="$(dirname "${target_path}")"
  ssh "root@${LAB_HOST}" "mkdir -p '$(printf "%q" "${target_dir}")'"
  scp "${source_path}" "root@${LAB_HOST}:${target_path}"
  ssh "root@${LAB_HOST}" "chown root:root '$(printf "%q" "${target_path}")'"
}

ssh "root@${LAB_HOST}" "
  if [ -f '${LEGACY_LAB_ROOT}/docker/compose/docker-compose.host-services.yml' ]; then
    LAB_ROOT='${LEGACY_LAB_ROOT}' LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' docker compose -p ckc-internal-lab -f '${LEGACY_LAB_ROOT}/docker/compose/docker-compose.host-services.yml' down --remove-orphans >/dev/null 2>&1 || true
  fi
  if [ -f '${LEGACY_LAB_ROOT}/compose/docker-compose.host-services.yml' ]; then
    LAB_ROOT='${LEGACY_LAB_ROOT}' LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' docker compose -p ckc-internal-lab -f '${LEGACY_LAB_ROOT}/compose/docker-compose.host-services.yml' down --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf '${LEGACY_LAB_ROOT}'
  rm -rf '${LAB_ROOT}/assets' '${LAB_ROOT}/workspace' '${LAB_ROOT}/shared' '${LAB_ROOT}/build-context' '${LAB_ROOT}/build' '${LAB_ROOT}/compose' '${LAB_ROOT}/runtime' '${LAB_ROOT}/images' '${LAB_ROOT}/fingerprints' '${LAB_ROOT}/generated' '${LAB_ROOT}/pids' '${LAB_ROOT}/audit' '${LAB_ROOT}/audit-tools' '${LAB_ROOT}/docker-compose.host-services.yml' '${LAB_ROOT}/process-exporter.yml' '${LAB_ROOT}/fluent-bit.yaml'
  mkdir -p '${LAB_ROOT}/config' '${LAB_ROOT}/docker/build/demo/build/install' '${LAB_ROOT}/docker/build/demo-stubs/build/install' '${LAB_ROOT}/load-test-runtime' '${LAB_ROOT}/state/images' '${LAB_ROOT}/state/fingerprints/images' '${LAB_ROOT}/state/pids' '${LAB_ROOT}/state/generated'
"
ssh "root@${LAB_HOST}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
EOF
ssh "root@${LAB_HOST}" "python3 -c 'import yaml' >/dev/null 2>&1 && command -v tcpdump >/dev/null 2>&1 && command -v tshark >/dev/null 2>&1 || (export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get install -y python3-yaml tcpdump tshark)"

DEMO_FINGERPRINT="$(image_fingerprint demo)"
DEMO_STUBS_FINGERPRINT="$(image_fingerprint demo-stubs)"
THREAD_STATS_AGENT_JAR_PATH="$(build_thread_stats_agent)"
if [[ -z "${THREAD_STATS_AGENT_JAR_PATH}" || ! -f "${THREAD_STATS_AGENT_JAR_PATH}" ]]; then
  echo "Thread Stats agent jar was not produced." >&2
  exit 1
fi
THREAD_STATS_AGENT_FINGERPRINT="$(sha256sum "${THREAD_STATS_AGENT_JAR_PATH}" | awk '{ print $1 }')"
LOAD_TEST_RUNTIME_FINGERPRINT="$(fingerprint_paths "load-test-runtime" \
  settings.gradle.kts \
  build.gradle.kts \
  gradle.properties \
  gradle/wrapper/gradle-wrapper.properties \
  demo/ckc-demo-contracts \
  demo/ckc-demo-load-test)"
ASSETS_SYNC_FINGERPRINT="$(fingerprint_paths "assets-sync" demo/infra/internal-lab/assets demo/infra/shared/helm)"
RUNTIME_TEST_ASSETS_FINGERPRINT="$(fingerprint_paths "runtime-test-assets" \
  demo/infra/shared/audit \
  demo/infra/shared/experiment_orchestration \
  demo/infra/shared/experiment_report \
  demo/infra/shared/pcap \
  demo/infra/shared/workloads \
  demo/infra/shared/grafana \
  demo/infra/internal-lab/workloads)"
BASE_DEPLOY_FINGERPRINT="$(fingerprint_paths "base-deploy" \
  demo/infra/internal-lab/assets/compose \
  demo/infra/internal-lab/assets/grafana \
  demo/infra/internal-lab/assets/k8s \
  demo/infra/internal-lab/assets/libexec/deploy-base.sh \
  demo/infra/shared/grafana \
  demo/infra/shared/helm/demo)"
STUBS_DEPLOY_FINGERPRINT="$(fingerprint_paths "stubs-deploy" \
  demo/infra/internal-lab/assets/config/demo-stubs-values.yaml \
  demo/infra/internal-lab/assets/libexec/deploy-stubs.sh \
  demo/infra/shared/helm/demo-stubs)"

DEMO_IMAGE_CHANGED=0
DEMO_STUBS_IMAGE_CHANGED=0
THREAD_STATS_AGENT_CHANGED=0
LOAD_TEST_RUNTIME_CHANGED=0
ASSETS_SYNC_CHANGED=0
RUNTIME_TEST_ASSETS_CHANGED=0
BASE_DEPLOY_CHANGED=0
STUBS_DEPLOY_CHANGED=0
DEMO_DEPLOY_RESTARTED=0

if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_image_is_current demo "${DEMO_FINGERPRINT}"; then
  DEMO_IMAGE_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_image_is_current demo-stubs "${DEMO_STUBS_FINGERPRINT}"; then
  DEMO_STUBS_IMAGE_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] ||
  ! remote_fingerprint_matches "thread-stats-agent" "${THREAD_STATS_AGENT_FINGERPRINT}" ||
  ! remote_paths_exist "${LAB_ROOT}/thread-stats/thread-stats-agent.jar"; then
  THREAD_STATS_AGENT_CHANGED=1
  BASE_DEPLOY_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "load-test-runtime" "${LOAD_TEST_RUNTIME_FINGERPRINT}"; then
  LOAD_TEST_RUNTIME_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "assets-sync" "${ASSETS_SYNC_FINGERPRINT}"; then
  ASSETS_SYNC_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] ||
  ! remote_fingerprint_matches "runtime-test-assets" "${RUNTIME_TEST_ASSETS_FINGERPRINT}" ||
  ! remote_paths_exist \
    "${LAB_ROOT}/helpers/audit/analyze-audit.py" \
    "${LAB_ROOT}/helpers/pcap/analyze-pcap.py" \
    "${LAB_ROOT}/workloads/consumer-profiles.yaml" \
    "${LAB_ROOT}/workloads/sla-profiles/consumer-baseline.yaml" \
    "${LAB_ROOT}/workloads/test-definitions/telemetry-freshness-fairness.yaml" \
    "${LAB_ROOT}/workloads/experiments/telemetry-fairness-profile-comparison.yaml" \
    "${LAB_ROOT}/workloads/experiments/spring-kafka-thread-stats-progression.yaml" \
    "${LAB_ROOT}/grafana/dashboards/ckc-overview.json"; then
  RUNTIME_TEST_ASSETS_CHANGED=1
fi
if [[ "${ASSETS_SYNC_CHANGED}" -eq 1 ]]; then
  # The base helper sync owns LAB_ROOT/helpers with --delete, so it can remove
  # shared runtime helpers such as helpers/audit even when their fingerprint
  # has not changed.
  RUNTIME_TEST_ASSETS_CHANGED=1
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
  sync_internal_lab_assets
  record_remote_fingerprint "assets-sync" "${ASSETS_SYNC_FINGERPRINT}"
fi

if [[ "${RUNTIME_TEST_ASSETS_CHANGED}" -eq 1 ]]; then
  sync_runtime_test_assets
  record_remote_fingerprint "runtime-test-assets" "${RUNTIME_TEST_ASSETS_FINGERPRINT}"
fi

if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  sync_file "${REPO_ROOT}/demo/ckc-demo/Dockerfile" "${LAB_ROOT}/docker/build/demo/Dockerfile"
  sync_path "${REPO_ROOT}/demo/ckc-demo/build/install/ckc-demo" "${LAB_ROOT}/docker/build/demo/build/install/ckc-demo"
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  sync_file "${REPO_ROOT}/demo/ckc-demo-stubs/Dockerfile" "${LAB_ROOT}/docker/build/demo-stubs/Dockerfile"
  sync_path "${REPO_ROOT}/demo/ckc-demo-stubs/build/install/ckc-demo-stubs" "${LAB_ROOT}/docker/build/demo-stubs/build/install/ckc-demo-stubs"
fi
if [[ "${LOAD_TEST_RUNTIME_CHANGED}" -eq 1 ]]; then
  sync_path "${REPO_ROOT}/demo/ckc-demo-load-test/build/install/ckc-demo-load-test" "${LAB_ROOT}/load-test-runtime"
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/load-test-runtime/bin/'*"
  record_remote_fingerprint "load-test-runtime" "${LOAD_TEST_RUNTIME_FINGERPRINT}"
fi
if [[ "${THREAD_STATS_AGENT_CHANGED}" -eq 1 ]]; then
  sync_file "${THREAD_STATS_AGENT_JAR_PATH}" "${LAB_ROOT}/thread-stats/thread-stats-agent.jar"
  record_remote_fingerprint "thread-stats-agent" "${THREAD_STATS_AGENT_FINGERPRINT}"
fi

if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/docker/build/demo/build/install/ckc-demo/bin/'*"
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  ssh "root@${LAB_HOST}" "chmod +x '${LAB_ROOT}/docker/build/demo-stubs/build/install/ckc-demo-stubs/bin/'*"
fi
if [[ "${BASE_DEPLOY_CHANGED}" -eq 1 ]]; then
  ssh "root@${LAB_HOST}" "LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/deploy-base.sh'"
  record_remote_fingerprint "base-deploy" "${BASE_DEPLOY_FINGERPRINT}"
fi

ssh "root@${LAB_HOST}" "find '${LAB_ROOT}' -mindepth 1 -maxdepth 1 ! -name prometheus -exec chown -R root:root {} +"

REBUILD_ARGS=()
if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  REBUILD_ARGS+=("demo=${DEMO_FINGERPRINT}")
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  REBUILD_ARGS+=("demo-stubs=${DEMO_STUBS_FINGERPRINT}")
fi
if [[ "${#REBUILD_ARGS[@]}" -gt 0 ]]; then
  ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/rebuild-images.sh' ${REBUILD_ARGS[*]}"
fi
if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  if ssh "root@${LAB_HOST}" "kubectl -n ckc-perf get deploy ckc-demo >/dev/null 2>&1"; then
    ssh "root@${LAB_HOST}" "kubectl -n ckc-perf rollout restart deploy/ckc-demo && kubectl -n ckc-perf rollout status deploy/ckc-demo --timeout=240s"
    DEMO_DEPLOY_RESTARTED=1
  fi
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]] || [[ "${STUBS_DEPLOY_CHANGED}" -eq 1 ]]; then
  if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
    ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/deploy-stubs.sh' --restart"
  else
    ssh "root@${LAB_HOST}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/deploy-stubs.sh'"
  fi
  record_remote_fingerprint "stubs-deploy" "${STUBS_DEPLOY_FINGERPRINT}"
fi

echo "Internal lab is updated."
echo "  demo image changed=${DEMO_IMAGE_CHANGED}"
echo "  demo-stubs image changed=${DEMO_STUBS_IMAGE_CHANGED}"
echo "  Thread Stats agent changed=${THREAD_STATS_AGENT_CHANGED}"
echo "  load-test runtime changed=${LOAD_TEST_RUNTIME_CHANGED}"
echo "  assets synced=${ASSETS_SYNC_CHANGED}"
echo "  runtime test assets synced=${RUNTIME_TEST_ASSETS_CHANGED}"
echo "  base redeployed=${BASE_DEPLOY_CHANGED}"
echo "  demo redeployed=${DEMO_DEPLOY_RESTARTED}"
echo "  demo-stubs redeployed=$(( DEMO_STUBS_IMAGE_CHANGED || STUBS_DEPLOY_CHANGED ))"
echo "  load-test runtime=${LAB_ROOT}/load-test-runtime"
echo "  lab entrypoints=${LAB_ROOT}/bin"
