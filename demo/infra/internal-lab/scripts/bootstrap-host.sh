#!/usr/bin/env bash

set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run bootstrap-host.sh through sudo or as root." >&2
  exit 1
fi

RUNTIME_USER=""
LAB_ROOT=""
NODE_IP=""
AUTHORIZED_KEY_FILES=()
PERFORMANCE_CPU_KHZ="2000000"
NODE_ROLE="server"
NODE_NAME=""
SERVER_ADDRESS=""
K3S_TOKEN_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runtime-user) RUNTIME_USER="${2:?--runtime-user requires a value}"; shift 2 ;;
    --lab-root) LAB_ROOT="${2:?--lab-root requires a value}"; shift 2 ;;
    --node-ip) NODE_IP="${2:?--node-ip requires a value}"; shift 2 ;;
    --authorized-key-file) AUTHORIZED_KEY_FILES+=("${2:?--authorized-key-file requires a value}"); shift 2 ;;
    --performance-cpu-khz) PERFORMANCE_CPU_KHZ="${2:?--performance-cpu-khz requires a value}"; shift 2 ;;
    --node-role) NODE_ROLE="${2:?--node-role requires a value}"; shift 2 ;;
    --node-name) NODE_NAME="${2:?--node-name requires a value}"; shift 2 ;;
    --server-address) SERVER_ADDRESS="${2:?--server-address requires a value}"; shift 2 ;;
    --k3s-token-file) K3S_TOKEN_FILE="${2:?--k3s-token-file requires a value}"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ ! "${RUNTIME_USER}" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]]; then
  echo "Invalid runtime user: ${RUNTIME_USER}" >&2
  exit 1
fi
if [[ "${LAB_ROOT}" != /* || "${LAB_ROOT}" == "/" || ! "${LAB_ROOT}" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
  echo "Lab root must be a non-root absolute path: ${LAB_ROOT}" >&2
  exit 1
fi
if [[ ! "${NODE_IP}" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "Invalid node address: ${NODE_IP}" >&2
  exit 1
fi
if [[ ! "${PERFORMANCE_CPU_KHZ}" =~ ^[0-9]+$ ]]; then
  echo "Performance CPU frequency must be a non-negative integer in kHz: ${PERFORMANCE_CPU_KHZ}" >&2
  exit 1
fi
if [[ "${NODE_ROLE}" != "server" && "${NODE_ROLE}" != "agent" ]]; then
  echo "Node role must be server or agent: ${NODE_ROLE}" >&2
  exit 1
fi
if [[ ! "${NODE_NAME}" =~ ^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$ ]]; then
  echo "Invalid node name: ${NODE_NAME}" >&2
  exit 1
fi
if [[ ! "${SERVER_ADDRESS}" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "Invalid k3s server address: ${SERVER_ADDRESS}" >&2
  exit 1
fi
if [[ "${NODE_ROLE}" == "agent" && ! -s "${K3S_TOKEN_FILE}" ]]; then
  echo "A readable k3s token file is required for an agent node." >&2
  exit 1
fi
if [[ "${#AUTHORIZED_KEY_FILES[@]}" -eq 0 ]]; then
  echo "At least one --authorized-key-file is required." >&2
  exit 1
fi
for authorized_key_file in "${AUTHORIZED_KEY_FILES[@]}"; do
  if [[ ! -s "${authorized_key_file}" ]]; then
    echo "SSH public key was not found: ${authorized_key_file}" >&2
    exit 1
  fi
done
if [[ ! -r /etc/os-release ]]; then
  echo "Ubuntu host detection failed: /etc/os-release is missing." >&2
  exit 1
fi
# shellcheck disable=SC1091
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" ]]; then
  echo "Internal-lab bootstrap currently supports Ubuntu; detected ${ID:-unknown}." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
  ca-certificates curl ethtool gnupg iproute2 iptables lsb-release \
  linux-tools-common linux-tools-generic openssh-server

if [[ "${NODE_ROLE}" == "server" ]]; then
  apt-get install -y openjdk-21-jre-headless python3-yaml rsync tcpdump tshark libcap2-bin
fi

if [[ "${NODE_ROLE}" == "server" ]] && ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now ssh
if [[ "${NODE_ROLE}" == "server" ]]; then
  systemctl enable --now docker
fi

if ! id "${RUNTIME_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "/var/lib/${RUNTIME_USER}" --shell /bin/bash "${RUNTIME_USER}"
fi
if [[ "${NODE_ROLE}" == "server" ]]; then
  usermod -aG docker "${RUNTIME_USER}"
fi
RUNTIME_HOME="$(getent passwd "${RUNTIME_USER}" | cut -d: -f6)"
RUNTIME_GROUP="$(id -gn "${RUNTIME_USER}")"

install -d -m 0700 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${RUNTIME_HOME}/.ssh"
touch "${RUNTIME_HOME}/.ssh/authorized_keys"
for authorized_key_file in "${AUTHORIZED_KEY_FILES[@]}"; do
  while IFS= read -r authorized_key; do
    [[ -z "${authorized_key}" ]] && continue
    if ! grep -qxF "${authorized_key}" "${RUNTIME_HOME}/.ssh/authorized_keys"; then
      printf '%s\n' "${authorized_key}" >> "${RUNTIME_HOME}/.ssh/authorized_keys"
    fi
  done < "${authorized_key_file}"
done
chown "${RUNTIME_USER}:${RUNTIME_GROUP}" "${RUNTIME_HOME}/.ssh/authorized_keys"
chmod 0600 "${RUNTIME_HOME}/.ssh/authorized_keys"
loginctl enable-linger "${RUNTIME_USER}" >/dev/null 2>&1 || true

if [[ "${NODE_ROLE}" == "server" ]]; then
  if systemctl cat k3s-agent.service >/dev/null 2>&1; then
    echo "This host is already configured as a k3s agent; uninstall it before configuring a server." >&2
    exit 1
  fi
  if ! command -v k3s >/dev/null 2>&1; then
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --disable traefik --disable servicelb --disable local-storage --disable metrics-server --write-kubeconfig-mode 600 --node-ip ${NODE_IP} --advertise-address ${NODE_IP} --node-name ${NODE_NAME} --node-label ckc.dev/role=controller" sh -
  else
    systemctl enable --now k3s
  fi
  timeout 120 bash -c 'until k3s kubectl get node "$1" >/dev/null 2>&1; do sleep 2; done' _ "${NODE_NAME}"
  k3s kubectl label node "${NODE_NAME}" ckc.dev/role=controller --overwrite
else
  if systemctl cat k3s.service >/dev/null 2>&1; then
    echo "This host is already configured as a k3s server; uninstall it before configuring an agent." >&2
    exit 1
  fi
  if ! command -v k3s >/dev/null 2>&1; then
    curl -sfL https://get.k3s.io | \
      K3S_URL="https://${SERVER_ADDRESS}:6443" K3S_TOKEN="$(<"${K3S_TOKEN_FILE}")" \
      INSTALL_K3S_EXEC="agent --node-ip ${NODE_IP} --node-name ${NODE_NAME} --node-label ckc.dev/role=application" sh -
  else
    systemctl enable --now k3s-agent
  fi
fi

swapoff -a || true
if [[ -f /swap.img ]]; then
  sed -i '/^[[:space:]]*\/swap.img[[:space:]]/ s/^/# disabled for internal lab: /' /etc/fstab
fi

install -d -m 0755 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${LAB_ROOT}"
for directory in config docker grafana helpers k8s load-test-runtime notify results state thread-stats; do
  install -d -m 0755 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${LAB_ROOT}/${directory}"
done
if [[ "${NODE_ROLE}" == "server" ]]; then
  install -d -m 0755 -o 65534 -g 65534 "${LAB_ROOT}/prometheus"
  install -d -m 0755 -o 10001 -g 10001 "${LAB_ROOT}/loki"
  install -d -m 0700 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${RUNTIME_HOME}/.kube"
  sed "s#https://127.0.0.1:6443#https://${NODE_IP}:6443#" /etc/rancher/k3s/k3s.yaml > "${RUNTIME_HOME}/.kube/config"
  chown "${RUNTIME_USER}:${RUNTIME_GROUP}" "${RUNTIME_HOME}/.kube/config"
  chmod 0600 "${RUNTIME_HOME}/.kube/config"
  timeout 120 bash -c 'until test -s /var/lib/rancher/k3s/server/node-token; do sleep 2; done'
  install -m 0600 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" /var/lib/rancher/k3s/server/node-token "${LAB_ROOT}/config/k3s-join-token"
  if [[ ! -f "${RUNTIME_HOME}/.ssh/id_ed25519" ]]; then
    runuser -u "${RUNTIME_USER}" -- ssh-keygen -q -t ed25519 -N '' -f "${RUNTIME_HOME}/.ssh/id_ed25519"
  fi
fi

install -d -m 0755 /etc/ckc-lab /usr/local/libexec/ckc-lab
cat > /etc/ckc-lab/host.env <<EOF
LAB_ROOT=${LAB_ROOT}
PERFORMANCE_CPU_KHZ=${PERFORMANCE_CPU_KHZ}
NODE_ROLE=${NODE_ROLE}
EOF
chmod 0644 /etc/ckc-lab/host.env
cat > /usr/local/libexec/ckc-lab/import-k3s-images <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" -ne 0 ]]; then
  echo "import-k3s-images does not accept arguments." >&2
  exit 2
fi
# shellcheck disable=SC1091
source /etc/ckc-lab/host.env
archive="${LAB_ROOT}/state/images/ckc-lab-images.tar"
if [[ ! -f "${archive}" ]]; then
  echo "Image archive was not found: ${archive}" >&2
  exit 1
fi
k3s ctr images import "${archive}"
EOF
chmod 0755 /usr/local/libexec/ckc-lab/import-k3s-images
cat > /usr/local/libexec/ckc-lab/cpu-performance-acquire <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" -ne 0 ]]; then
  echo "cpu-performance-acquire does not accept arguments." >&2
  exit 2
fi
# shellcheck disable=SC1091
source /etc/ckc-lab/host.env
STATE_DIR=/run/ckc-lab
STATE_FILE=${STATE_DIR}/cpu-policy.snapshot
LOCK_FILE=${STATE_DIR}/cpu-policy.lock
install -d -m 0755 -o root -g root "${STATE_DIR}"
exec 9>"${LOCK_FILE}"
flock -x 9
if [[ -e "${STATE_FILE}" ]]; then
  echo "A CKC CPU performance lease is already active." >&2
  exit 1
fi
if [[ "${PERFORMANCE_CPU_KHZ}" -eq 0 ]]; then
  printf 'disabled\n' > "${STATE_FILE}"
  exit 0
fi
policies=(/sys/devices/system/cpu/cpufreq/policy*)
if [[ ! -e "${policies[0]}" ]]; then
  printf 'unavailable\n' > "${STATE_FILE}"
  exit 0
fi
temporary="${STATE_FILE}.tmp"
: > "${temporary}"
for policy in "${policies[@]}"; do
  minimum="$(<"${policy}/scaling_min_freq")"
  maximum="$(<"${policy}/scaling_max_freq")"
  governor="$(<"${policy}/scaling_governor")"
  hardware_minimum="$(<"${policy}/cpuinfo_min_freq")"
  hardware_maximum="$(<"${policy}/cpuinfo_max_freq")"
  if (( PERFORMANCE_CPU_KHZ < hardware_minimum || PERFORMANCE_CPU_KHZ > hardware_maximum )); then
    rm -f "${temporary}"
    echo "Requested ${PERFORMANCE_CPU_KHZ} kHz is outside ${policy} hardware range ${hardware_minimum}-${hardware_maximum}." >&2
    exit 1
  fi
  printf '%s|%s|%s|%s\n' "${policy}" "${minimum}" "${maximum}" "${governor}" >> "${temporary}"
done
mv "${temporary}" "${STATE_FILE}"
restore_on_error() {
  flock -u 9
  /usr/local/libexec/ckc-lab/cpu-performance-release || true
}
trap restore_on_error ERR
for policy in "${policies[@]}"; do
  printf '%s\n' performance > "${policy}/scaling_governor"
  hardware_minimum="$(<"${policy}/cpuinfo_min_freq")"
  printf '%s\n' "${hardware_minimum}" > "${policy}/scaling_min_freq"
  printf '%s\n' "${PERFORMANCE_CPU_KHZ}" > "${policy}/scaling_max_freq"
  printf '%s\n' "${PERFORMANCE_CPU_KHZ}" > "${policy}/scaling_min_freq"
done
trap - ERR
echo "CKC CPU performance lease acquired at ${PERFORMANCE_CPU_KHZ} kHz."
EOF
chmod 0755 /usr/local/libexec/ckc-lab/cpu-performance-acquire
cat > /usr/local/libexec/ckc-lab/cpu-performance-release <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" -ne 0 ]]; then
  echo "cpu-performance-release does not accept arguments." >&2
  exit 2
fi
STATE_DIR=/run/ckc-lab
STATE_FILE=${STATE_DIR}/cpu-policy.snapshot
LOCK_FILE=${STATE_DIR}/cpu-policy.lock
install -d -m 0755 -o root -g root "${STATE_DIR}"
exec 9>"${LOCK_FILE}"
flock -x 9
if [[ ! -e "${STATE_FILE}" ]]; then
  exit 0
fi
if grep -qxE 'disabled|unavailable' "${STATE_FILE}"; then
  rm -f "${STATE_FILE}"
  exit 0
fi
while IFS='|' read -r policy minimum maximum governor; do
  [[ -d "${policy}" ]] || continue
  hardware_maximum="$(<"${policy}/cpuinfo_max_freq")"
  printf '%s\n' "${hardware_maximum}" > "${policy}/scaling_max_freq"
  printf '%s\n' "${minimum}" > "${policy}/scaling_min_freq"
  printf '%s\n' "${maximum}" > "${policy}/scaling_max_freq"
  printf '%s\n' "${governor}" > "${policy}/scaling_governor"
done < "${STATE_FILE}"
rm -f "${STATE_FILE}"
echo "CKC CPU performance lease released and the previous policy restored."
EOF
chmod 0755 /usr/local/libexec/ckc-lab/cpu-performance-release
cat > /etc/sudoers.d/ckc-lab <<EOF
${RUNTIME_USER} ALL=(root) NOPASSWD: /usr/local/libexec/ckc-lab/import-k3s-images "", /usr/local/libexec/ckc-lab/cpu-performance-acquire "", /usr/local/libexec/ckc-lab/cpu-performance-release ""
EOF
chmod 0440 /etc/sudoers.d/ckc-lab
visudo -cf /etc/sudoers.d/ckc-lab >/dev/null

if [[ "${NODE_ROLE}" == "server" ]]; then
  setcap cap_net_admin,cap_net_raw=eip "$(command -v tcpdump)"
fi

echo "Internal-lab host bootstrap is complete."
echo "  runtime_user=${RUNTIME_USER}"
echo "  runtime_home=${RUNTIME_HOME}"
echo "  lab_root=${LAB_ROOT}"
echo "  node_ip=${NODE_IP}"
echo "  node_role=${NODE_ROLE}"
