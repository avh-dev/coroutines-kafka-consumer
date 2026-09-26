#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.demo-infra/internal-lab"
DEFAULT_LAB_ROOT="/opt/ckc-lab"
LEGACY_LAB_ROOT="/opt/ckc-internal-lab"
LEGACY_FINGERPRINT_LOCALE="${LC_ALL:-${LC_COLLATE:-${LANG:-C}}}"
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

fingerprint_paths_with_locale() {
  local sort_locale="$1"
  local label="$2"
  shift 2

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
            | LC_ALL="${sort_locale}" sort -z \
            | xargs -0 sha256sum
        fi
      done
    } | sha256sum | awk '{ print $1 }'
  )
}

fingerprint_paths() {
  fingerprint_paths_with_locale C "$@"
}

legacy_fingerprint_paths() {
  fingerprint_paths_with_locale "${LEGACY_FINGERPRINT_LOCALE}" "$@"
}

image_fingerprint() {
  local service="$1"
  local fingerprint_function="${2:-fingerprint_paths}"
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

  "${fingerprint_function}" "image-v3-${service}" "${paths[@]}"
}

remote_image_is_current() {
  local service="$1"
  local fingerprint="$2"

  ssh "${LAB_TARGET}" \
    "test \"\$(cat '${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}'"
}

worker_image_is_current() {
  local service="$1"
  local fingerprint="$2"

  [[ -z "${LAB_APPLICATION_TARGET:-}" ]] && return 0
  ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "${LAB_APPLICATION_TARGET}" \
    "test \"\$(cat '${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}'"
}

remote_fingerprint_matches() {
  local name="$1"
  local fingerprint="$2"

  ssh "${LAB_TARGET}" \
    "test \"\$(cat '${LAB_ROOT}/state/fingerprints/${name}.fingerprint' 2>/dev/null || true)\" = '${fingerprint}'"
}

remote_paths_exist() {
  local -a paths=("$@")
  local command="set -e;"
  local path

  for path in "${paths[@]}"; do
    command="${command} test -e '$(printf "%q" "${path}")';"
  done
  ssh "${LAB_TARGET}" "${command}"
}

resolve_thread_stats_agent() {
  if [[ -n "${THREAD_STATS_AGENT_JAR:-}" ]]; then
    if [[ ! -f "${THREAD_STATS_AGENT_JAR}" ]]; then
      echo "THREAD_STATS_AGENT_JAR does not exist: ${THREAD_STATS_AGENT_JAR}" >&2
      exit 1
    fi
    printf "%s\n" "${THREAD_STATS_AGENT_JAR}"
    return
  fi

  local version maven_repository maven_jar staged_jar
  version="$(sed -n 's/^threadStatsVersion=//p' "${REPO_ROOT}/gradle.properties" | tail -n 1)"
  if [[ -z "${version}" ]]; then
    echo "threadStatsVersion is missing from gradle.properties." >&2
    exit 1
  fi
  maven_repository="${MAVEN_REPO_LOCAL:-${HOME}/.m2/repository}"
  maven_jar="${maven_repository}/dev/avh/threadstats/thread-stats-agent/${version}/thread-stats-agent-${version}.jar"
  staged_jar="${REPO_ROOT}/build/internal-lab/thread-stats-agent/${version}/thread-stats-agent.jar"
  if [[ -f "${maven_jar}" ]]; then
    printf '%s\n' "${maven_jar}"
    return
  fi
  if [[ ! -f "${staged_jar}" ]]; then
    echo "Resolving Thread Stats agent dev.avh.threadstats:thread-stats-agent:${version}." >&2
    (cd "${REPO_ROOT}" && ./gradlew --quiet stageThreadStatsAgent >&2)
  fi
  if [[ ! -f "${staged_jar}" ]]; then
    echo "Gradle did not resolve the Thread Stats agent: ${staged_jar}" >&2
    exit 1
  fi
  printf '%s\n' "${staged_jar}"
}

record_remote_fingerprint() {
  local name="$1"
  local fingerprint="$2"

  ssh "${LAB_TARGET}" "mkdir -p '${LAB_ROOT}/state/fingerprints' && printf '%s\n' '${fingerprint}' > '${LAB_ROOT}/state/fingerprints/${name}.fingerprint'"
}

record_remote_image_fingerprint() {
  local service="$1"
  local fingerprint="$2"

  ssh "${LAB_TARGET}" "mkdir -p '${LAB_ROOT}/state/fingerprints/images' && printf '%s\n' '${fingerprint}' > '${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint'"
  if [[ -n "${LAB_APPLICATION_TARGET:-}" ]]; then
    ssh "${LAB_APPLICATION_TARGET}" "mkdir -p '${LAB_ROOT}/state/fingerprints/images' && printf '%s\n' '${fingerprint}' > '${LAB_ROOT}/state/fingerprints/images/${service}.fingerprint'"
  fi
}

sync_internal_lab_assets() {
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/bin" "${LAB_ROOT}/bin"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/libexec" "${LAB_ROOT}/libexec"
  ssh "${LAB_TARGET}" "mkdir -p '${LAB_ROOT}/helpers'"
  for helper in "${REPO_ROOT}/demo/infra/internal-lab/assets/helpers/"*.py; do
    sync_file "${helper}" "${LAB_ROOT}/helpers/$(basename "${helper}")"
  done
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/compose" "${LAB_ROOT}/docker/compose"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/k8s" "${LAB_ROOT}/k8s"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/systemd" "${LAB_ROOT}/systemd"
  ssh "${LAB_TARGET}" "mkdir -p '${LAB_ROOT}/notify'"
  sync_file "${REPO_ROOT}/demo/infra/internal-lab/assets/notify/README.md" "${LAB_ROOT}/notify/README.md"
  sync_file "${REPO_ROOT}/demo/infra/internal-lab/assets/notify/notify-telegram.py" "${LAB_ROOT}/notify/notify-telegram.py"
  sync_file "${REPO_ROOT}/demo/infra/internal-lab/assets/notify/notify.sh" "${LAB_ROOT}/notify/notify.sh"
  sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/grafana" "${LAB_ROOT}/grafana/templates"
  ssh "${LAB_TARGET}" "chmod +x '${LAB_ROOT}/bin/'*.sh '${LAB_ROOT}/libexec/'*.sh '${LAB_ROOT}/libexec/'*.py '${LAB_ROOT}/helpers/result_bundle/restore/'*.sh '${LAB_ROOT}/helpers/result_bundle/restore/'*.py '${LAB_ROOT}/notify/'*.sh '${LAB_ROOT}/notify/'*.py 2>/dev/null || true"
}

sync_runtime_test_assets() {
  local materialized_dashboard="${STATE_DIR}/grafana/ckc-overview.json"

  mkdir -p "$(dirname -- "${materialized_dashboard}")"
  python3 "${REPO_ROOT}/demo/infra/shared/result_bundle/dashboard.py" \
    "${REPO_ROOT}/demo/infra/shared/grafana/dashboards/ckc-overview.json" \
    "${materialized_dashboard}" \
    --environment internal-lab \
    --kafka-mode kubernetes
  sync_path "${REPO_ROOT}/demo/infra/shared/audit" "${LAB_ROOT}/helpers/audit"
  sync_file "${REPO_ROOT}/demo/infra/shared/audit_windows.py" "${LAB_ROOT}/helpers/audit_windows.py"
  sync_path "${REPO_ROOT}/demo/infra/shared/pcap" "${LAB_ROOT}/helpers/pcap"
  sync_path "${REPO_ROOT}/demo/infra/shared/experiment_orchestration" "${LAB_ROOT}/helpers/experiment_orchestration"
  sync_path "${REPO_ROOT}/demo/infra/shared/experiment_notifications" "${LAB_ROOT}/helpers/experiment_notifications"
  sync_path "${REPO_ROOT}/demo/infra/shared/kafka_warmup" "${LAB_ROOT}/helpers/kafka_warmup"
  sync_path "${REPO_ROOT}/demo/infra/shared/experiment_report" "${LAB_ROOT}/helpers/experiment_report"
  sync_path "${REPO_ROOT}/demo/infra/shared/result_bundle" "${LAB_ROOT}/helpers/result_bundle"
  sync_path "${REPO_ROOT}/demo/infra/experiments" "${LAB_ROOT}/experiments"
  ssh "${LAB_TARGET}" "mkdir -p '${LAB_ROOT}/grafana/dashboards'"
  sync_file "${materialized_dashboard}" "${LAB_ROOT}/grafana/dashboards/ckc-overview.json"
  sync_path "${REPO_ROOT}/demo/infra/shared/grafana/provisioning/dashboards" "${LAB_ROOT}/grafana/provisioning/dashboards"
  ssh "${LAB_TARGET}" "rm -rf '${LAB_ROOT}/test-definitions' '${LAB_ROOT}/variants' '${LAB_ROOT}/test-bundles'"
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

LAB_ENV_PATH="${CKC_LAB_ENV:-${STATE_DIR}/lab.env}"
if [[ ! -f "${LAB_ENV_PATH}" ]]; then
  echo "Lab state was not found: ${LAB_ENV_PATH}. Run demo/infra/internal-lab/scripts/lab.sh init first." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${LAB_ENV_PATH}"

LAB_ROOT="${LAB_ROOT:-${DEFAULT_LAB_ROOT}}"
if [[ "${LAB_ROOT}" = "${LEGACY_LAB_ROOT}" ]]; then
  LAB_ROOT="${DEFAULT_LAB_ROOT}"
fi
LAB_HOST="${LAB_HOST:-}"
if [[ -z "${LAB_HOST:-}" ]]; then
  echo "LAB_HOST is missing from ${LAB_ENV_PATH}." >&2
  exit 1
fi
LAB_SSH_HOST="${LAB_SSH_HOST:-${LAB_HOST}}"
LAB_USER="${LAB_USER:-ckc-lab}"
LAB_TARGET="${LAB_USER}@${LAB_SSH_HOST}"
LAB_NODE_IP="${LAB_NODE_IP:-$(resolve_host_ip "${LAB_HOST}")}"
LAB_TOPOLOGY="${LAB_TOPOLOGY:-single-host}"
if [[ -z "${LAB_APPLICATION_LINK:-}" ]]; then
  if [[ "${LAB_TOPOLOGY}" == "split-application" ]]; then
    LAB_APPLICATION_LINK="lan"
  else
    LAB_APPLICATION_LINK="local"
  fi
fi
LAB_APPLICATION_HOST="${LAB_APPLICATION_HOST:-${LAB_NODE_IP}}"
LAB_APPLICATION_TARGET="${LAB_APPLICATION_TARGET:-}"
LAB_APPLICATION_NODE_SELECTOR="${LAB_APPLICATION_NODE_SELECTOR:-}"
LAB_CONTROLLER_NODE_SELECTOR="${LAB_CONTROLLER_NODE_SELECTOR:-}"
if [[ -z "${LAB_NODE_IP}" ]]; then
  echo "Unable to resolve lab host: ${LAB_HOST}" >&2
  exit 1
fi
DEMO_FINGERPRINT="$(image_fingerprint demo)"
DEMO_STUBS_FINGERPRINT="$(image_fingerprint demo-stubs)"
THREAD_STATS_AGENT_JAR_PATH="$(resolve_thread_stats_agent)"
if [[ -z "${THREAD_STATS_AGENT_JAR_PATH}" || ! -f "${THREAD_STATS_AGENT_JAR_PATH}" ]]; then
  echo "Thread Stats agent jar was not resolved." >&2
  exit 1
fi
THREAD_STATS_AGENT_FINGERPRINT="$(sha256sum "${THREAD_STATS_AGENT_JAR_PATH}" | awk '{ print $1 }')"
THREAD_STATS_VERSION="$(sed -n 's/^threadStatsVersion=//p' "${REPO_ROOT}/gradle.properties" | tail -n 1)"
THREAD_STATS_LOCAL_RUNTIME_FINGERPRINT="$({
  found=0
  for module in thread-stats-core thread-stats-micrometer thread-stats-spring-boot-starter; do
    artifact="${MAVEN_REPO_LOCAL:-${HOME}/.m2/repository}/dev/avh/threadstats/${module}/${THREAD_STATS_VERSION}/${module}-${THREAD_STATS_VERSION}.jar"
    if [[ -f "${artifact}" ]]; then
      sha256sum "${artifact}"
      found=1
    fi
  done
  if [[ "${found}" -eq 0 ]]; then
    printf '%s\n' "release:${THREAD_STATS_VERSION}"
  fi
} | sha256sum | awk '{ print $1 }')"
DEMO_FINGERPRINT="$(printf '%s\n%s\n' "${DEMO_FINGERPRINT}" "${THREAD_STATS_LOCAL_RUNTIME_FINGERPRINT}" | sha256sum | awk '{ print $1 }')"
LOAD_TEST_RUNTIME_FINGERPRINT="$(fingerprint_paths "load-test-runtime" \
  settings.gradle.kts \
  build.gradle.kts \
  gradle.properties \
  gradle/wrapper/gradle-wrapper.properties \
  demo/ckc-demo-contracts \
  demo/ckc-demo-load-test)"
ASSETS_SYNC_FINGERPRINT="$(fingerprint_paths "assets-sync" demo/infra/internal-lab/assets)"
RUNTIME_TEST_ASSETS_FINGERPRINT="$(fingerprint_paths "runtime-test-assets" \
  demo/infra/shared/audit \
  demo/infra/shared/audit_windows.py \
  demo/infra/shared/experiment_orchestration \
  demo/infra/shared/experiment_notifications \
  demo/infra/shared/kafka_warmup \
  demo/infra/shared/experiment_report \
  demo/infra/shared/pcap \
  demo/infra/shared/result_bundle \
  demo/infra/shared/grafana \
  demo/infra/experiments)"
BASE_DEPLOY_FINGERPRINT="$(fingerprint_paths "base-deploy" \
  demo/infra/internal-lab/assets/compose \
  demo/infra/internal-lab/assets/grafana \
  demo/infra/internal-lab/assets/k8s \
  demo/infra/internal-lab/assets/libexec/deploy-base.sh \
  demo/infra/shared/grafana)"
UPDATE_FINGERPRINT="$({
  printf '%s\n' "internal-lab-update-v2"
  printf '%s\n' "${DEMO_FINGERPRINT}" "${DEMO_STUBS_FINGERPRINT}" "${THREAD_STATS_AGENT_FINGERPRINT}"
  printf '%s\n' "${LOAD_TEST_RUNTIME_FINGERPRINT}" "${ASSETS_SYNC_FINGERPRINT}" "${RUNTIME_TEST_ASSETS_FINGERPRINT}" "${BASE_DEPLOY_FINGERPRINT}"
  printf '%s\n' "${LAB_HOST}" "${LAB_NODE_IP}" "${LAB_ROOT}" "${LAB_TOPOLOGY}" "${LAB_APPLICATION_LINK}"
  printf '%s\n' "${LAB_APPLICATION_HOST}" "${LAB_APPLICATION_TARGET}" "${LAB_APPLICATION_NODE_SELECTOR}" "${LAB_CONTROLLER_NODE_SELECTOR}"
} | sha256sum | awk '{ print $1 }')"

# INFRA-218 initially inherited the caller's collation order. Retain one
# compatibility calculation so an installation written by that version can be
# migrated to locale-independent fingerprints without rebuilding artifacts.
LEGACY_DEMO_FINGERPRINT="$(image_fingerprint demo legacy_fingerprint_paths)"
LEGACY_DEMO_STUBS_FINGERPRINT="$(image_fingerprint demo-stubs legacy_fingerprint_paths)"
LEGACY_DEMO_FINGERPRINT="$(printf '%s\n%s\n' "${LEGACY_DEMO_FINGERPRINT}" "${THREAD_STATS_LOCAL_RUNTIME_FINGERPRINT}" | sha256sum | awk '{ print $1 }')"
LEGACY_LOAD_TEST_RUNTIME_FINGERPRINT="$(legacy_fingerprint_paths "load-test-runtime" \
  settings.gradle.kts \
  build.gradle.kts \
  gradle.properties \
  gradle/wrapper/gradle-wrapper.properties \
  demo/ckc-demo-contracts \
  demo/ckc-demo-load-test)"
LEGACY_ASSETS_SYNC_FINGERPRINT="$(legacy_fingerprint_paths "assets-sync" demo/infra/internal-lab/assets)"
LEGACY_RUNTIME_TEST_ASSETS_FINGERPRINT="$(legacy_fingerprint_paths "runtime-test-assets" \
  demo/infra/shared/audit \
  demo/infra/shared/audit_windows.py \
  demo/infra/shared/experiment_orchestration \
  demo/infra/shared/experiment_notifications \
  demo/infra/shared/kafka_warmup \
  demo/infra/shared/experiment_report \
  demo/infra/shared/pcap \
  demo/infra/shared/result_bundle \
  demo/infra/shared/grafana \
  demo/infra/experiments)"
LEGACY_BASE_DEPLOY_FINGERPRINT="$(legacy_fingerprint_paths "base-deploy" \
  demo/infra/internal-lab/assets/compose \
  demo/infra/internal-lab/assets/grafana \
  demo/infra/internal-lab/assets/k8s \
  demo/infra/internal-lab/assets/libexec/deploy-base.sh \
  demo/infra/shared/grafana)"
LEGACY_UPDATE_FINGERPRINT="$({
  printf '%s\n' "internal-lab-update-v2"
  printf '%s\n' "${LEGACY_DEMO_FINGERPRINT}" "${LEGACY_DEMO_STUBS_FINGERPRINT}" "${THREAD_STATS_AGENT_FINGERPRINT}"
  printf '%s\n' "${LEGACY_LOAD_TEST_RUNTIME_FINGERPRINT}" "${LEGACY_ASSETS_SYNC_FINGERPRINT}" "${LEGACY_RUNTIME_TEST_ASSETS_FINGERPRINT}" "${LEGACY_BASE_DEPLOY_FINGERPRINT}"
  printf '%s\n' "${LAB_HOST}" "${LAB_NODE_IP}" "${LAB_ROOT}" "${LAB_TOPOLOGY}" "${LAB_APPLICATION_LINK}"
  printf '%s\n' "${LAB_APPLICATION_HOST}" "${LAB_APPLICATION_TARGET}" "${LAB_APPLICATION_NODE_SELECTOR}" "${LAB_CONTROLLER_NODE_SELECTOR}"
} | sha256sum | awk '{ print $1 }')"

REMOTE_PROBE="$(ssh "${LAB_TARGET}" "
  if systemctl --user is-active --quiet ckc-experiment.service; then printf 'active'; else printf 'inactive'; fi
  printf '|'
  if test -f '${LAB_ROOT}/state/fingerprints/update.fingerprint'; then tr -d '\\n' < '${LAB_ROOT}/state/fingerprints/update.fingerprint'; fi
  printf '|'
  if test -x '${LAB_ROOT}/bin/run-experiment.sh' && test -f '${LAB_ROOT}/thread-stats/thread-stats-agent.jar'; then printf 'complete'; else printf 'incomplete'; fi
")"
if [[ "${REMOTE_PROBE}" == active\|* ]]; then
  echo "A managed experiment is active on ${LAB_TARGET}; stop it before updating the lab." >&2
  exit 1
fi
if [[ "${FORCE_REBUILD}" -eq 0 && "${REMOTE_PROBE}" == "inactive|${UPDATE_FINGERPRINT}|complete" ]]; then
  echo "Internal lab is already current (${UPDATE_FINGERPRINT:0:12}); no files transferred."
  exit 0
fi
if [[ "${FORCE_REBUILD}" -eq 0 && "${UPDATE_FINGERPRINT}" != "${LEGACY_UPDATE_FINGERPRINT}" && "${REMOTE_PROBE}" == "inactive|${LEGACY_UPDATE_FINGERPRINT}|complete" ]]; then
  record_remote_image_fingerprint demo "${DEMO_FINGERPRINT}"
  record_remote_image_fingerprint demo-stubs "${DEMO_STUBS_FINGERPRINT}"
  record_remote_fingerprint "thread-stats-agent" "${THREAD_STATS_AGENT_FINGERPRINT}"
  record_remote_fingerprint "load-test-runtime" "${LOAD_TEST_RUNTIME_FINGERPRINT}"
  record_remote_fingerprint "assets-sync" "${ASSETS_SYNC_FINGERPRINT}"
  record_remote_fingerprint "runtime-test-assets" "${RUNTIME_TEST_ASSETS_FINGERPRINT}"
  record_remote_fingerprint "base-deploy" "${BASE_DEPLOY_FINGERPRINT}"
  record_remote_fingerprint "update" "${UPDATE_FINGERPRINT}"
  echo "Migrated locale-dependent lab fingerprints (${UPDATE_FINGERPRINT:0:12}); no files transferred."
  exit 0
fi
if [[ -n "${LAB_APPLICATION_TARGET}" ]]; then
  if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "${LAB_APPLICATION_TARGET}" \
    "systemctl is-active --quiet k3s-agent && test -x /usr/local/libexec/ckc-lab/import-k3s-images"; then
    echo "Application worker is not ready at ${LAB_APPLICATION_TARGET}; run lab.sh bootstrap first." >&2
    exit 1
  fi
fi
sync_path() {
  local source_path="$1"
  local target_path="$2"

  ssh "${LAB_TARGET}" "mkdir -p '$(printf "%q" "${target_path}")'"

  if command -v rsync >/dev/null 2>&1; then
    rsync -az --delete --no-owner --no-group --exclude '__pycache__/' --exclude '*.pyc' "${source_path%/}/" "${LAB_TARGET}:${target_path%/}/"
    return
  fi

  echo "rsync was not found; using tar over ssh for ${source_path}."
  ssh "${LAB_TARGET}" "rm -rf '$(printf "%q" "${target_path}")' && mkdir -p '$(printf "%q" "${target_path}")'"
  tar --exclude='__pycache__' --exclude='*.pyc' -C "${source_path}" -cf - . | ssh "${LAB_TARGET}" "tar --no-same-owner -C '${target_path}' -xf -"
}

sync_file() {
  local source_path="$1"
  local target_path="$2"
  local target_dir

  target_dir="$(dirname "${target_path}")"
  ssh "${LAB_TARGET}" "mkdir -p '$(printf "%q" "${target_dir}")'"
  if command -v rsync >/dev/null 2>&1; then
    rsync -az --no-owner --no-group "${source_path}" "${LAB_TARGET}:${target_path}"
  else
    scp "${source_path}" "${LAB_TARGET}:${target_path}"
  fi
}

ssh "${LAB_TARGET}" "
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
ssh "${LAB_TARGET}" "cat > '${LAB_ROOT}/config/lab.env'" <<EOF
LAB_HOST=${LAB_HOST}
LAB_NODE_IP=${LAB_NODE_IP}
LAB_ROOT=${LAB_ROOT}
LAB_TOPOLOGY=${LAB_TOPOLOGY}
LAB_APPLICATION_LINK=${LAB_APPLICATION_LINK}
LAB_APPLICATION_HOST=${LAB_APPLICATION_HOST}
LAB_APPLICATION_TARGET=${LAB_APPLICATION_TARGET}
LAB_APPLICATION_NODE_SELECTOR=${LAB_APPLICATION_NODE_SELECTOR}
LAB_CONTROLLER_NODE_SELECTOR=${LAB_CONTROLLER_NODE_SELECTOR}
EOF
if ! ssh "${LAB_TARGET}" "python3 -c 'import yaml' >/dev/null 2>&1 && command -v tcpdump >/dev/null 2>&1 && command -v tshark >/dev/null 2>&1"; then
  echo "Lab prerequisites are missing on ${LAB_TARGET}; run lab.sh bootstrap before lab.sh up." >&2
  exit 1
fi

DEMO_IMAGE_CHANGED=0
DEMO_STUBS_IMAGE_CHANGED=0
THREAD_STATS_AGENT_CHANGED=0
LOAD_TEST_RUNTIME_CHANGED=0
ASSETS_SYNC_CHANGED=0
RUNTIME_TEST_ASSETS_CHANGED=0
BASE_DEPLOY_CHANGED=0
DEMO_DEPLOY_RESTARTED=0

if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_image_is_current demo "${DEMO_FINGERPRINT}" || ! worker_image_is_current demo "${DEMO_FINGERPRINT}"; then
  DEMO_IMAGE_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_image_is_current demo-stubs "${DEMO_STUBS_FINGERPRINT}" || ! worker_image_is_current demo-stubs "${DEMO_STUBS_FINGERPRINT}"; then
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
    "${LAB_ROOT}/helpers/audit_windows.py" \
    "${LAB_ROOT}/helpers/pcap/analyze-pcap.py" \
    "${LAB_ROOT}/experiments/telemetry-fairness-profile-comparison.yaml" \
    "${LAB_ROOT}/experiments/spring-kafka-thread-stats-progression.yaml" \
    "${LAB_ROOT}/grafana/dashboards/ckc-overview.json"; then
  RUNTIME_TEST_ASSETS_CHANGED=1
fi
if [[ "${FORCE_REBUILD}" -eq 1 ]] || ! remote_fingerprint_matches "base-deploy" "${BASE_DEPLOY_FINGERPRINT}"; then
  BASE_DEPLOY_CHANGED=1
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

ssh "${LAB_TARGET}" "LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/install-user-service.sh'"

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
  ssh "${LAB_TARGET}" "chmod +x '${LAB_ROOT}/load-test-runtime/bin/'*"
  record_remote_fingerprint "load-test-runtime" "${LOAD_TEST_RUNTIME_FINGERPRINT}"
fi
if [[ "${THREAD_STATS_AGENT_CHANGED}" -eq 1 ]]; then
  sync_file "${THREAD_STATS_AGENT_JAR_PATH}" "${LAB_ROOT}/thread-stats/thread-stats-agent.jar"
  record_remote_fingerprint "thread-stats-agent" "${THREAD_STATS_AGENT_FINGERPRINT}"
fi

if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  ssh "${LAB_TARGET}" "chmod +x '${LAB_ROOT}/docker/build/demo/build/install/ckc-demo/bin/'*"
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]]; then
  ssh "${LAB_TARGET}" "chmod +x '${LAB_ROOT}/docker/build/demo-stubs/build/install/ckc-demo-stubs/bin/'*"
fi
if [[ "${BASE_DEPLOY_CHANGED}" -eq 1 ]]; then
  ssh "${LAB_TARGET}" "LAB_NODE_IP='${LAB_NODE_IP}' LAB_HOST='${LAB_HOST}' LAB_ROOT='${LAB_ROOT}' '${LAB_ROOT}/libexec/deploy-base.sh'"
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
  ssh "${LAB_TARGET}" "LAB_ROOT='${LAB_ROOT}' LAB_APPLICATION_TARGET='${LAB_APPLICATION_TARGET}' '${LAB_ROOT}/libexec/rebuild-images.sh' ${REBUILD_ARGS[*]}"
fi
if [[ "${DEMO_IMAGE_CHANGED}" -eq 1 ]]; then
  if ssh "${LAB_TARGET}" "KUBECONFIG=\"\$HOME/.kube/config\" kubectl -n ckc-perf get deploy ckc-demo >/dev/null 2>&1"; then
    ssh "${LAB_TARGET}" "export KUBECONFIG=\"\$HOME/.kube/config\"; kubectl -n ckc-perf rollout restart deploy/ckc-demo && kubectl -n ckc-perf rollout status deploy/ckc-demo --timeout=240s"
    DEMO_DEPLOY_RESTARTED=1
  fi
fi
if [[ "${DEMO_STUBS_IMAGE_CHANGED}" -eq 1 ]] && ssh "${LAB_TARGET}" "KUBECONFIG=\"\$HOME/.kube/config\" kubectl -n ckc-perf get deploy ckc-demo-stubs >/dev/null 2>&1"; then
  ssh "${LAB_TARGET}" "export KUBECONFIG=\"\$HOME/.kube/config\"; kubectl -n ckc-perf rollout restart deploy/ckc-demo-stubs && kubectl -n ckc-perf rollout status deploy/ckc-demo-stubs --timeout=240s"
fi

record_remote_fingerprint "update" "${UPDATE_FINGERPRINT}"

echo "Internal lab is updated."
echo "  demo image changed=${DEMO_IMAGE_CHANGED}"
echo "  demo-stubs image changed=${DEMO_STUBS_IMAGE_CHANGED}"
echo "  Thread Stats agent changed=${THREAD_STATS_AGENT_CHANGED}"
echo "  load-test runtime changed=${LOAD_TEST_RUNTIME_CHANGED}"
echo "  assets synced=${ASSETS_SYNC_CHANGED}"
echo "  runtime test assets synced=${RUNTIME_TEST_ASSETS_CHANGED}"
echo "  base redeployed=${BASE_DEPLOY_CHANGED}"
echo "  demo redeployed=${DEMO_DEPLOY_RESTARTED}"
echo "  demo-stubs restarted=${DEMO_STUBS_IMAGE_CHANGED}"
echo "  load-test runtime=${LAB_ROOT}/load-test-runtime"
echo "  lab entrypoints=${LAB_ROOT}/bin"
