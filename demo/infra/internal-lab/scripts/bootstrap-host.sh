#!/usr/bin/env bash

set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run bootstrap-host.sh through sudo or as root." >&2
  exit 1
fi

RUNTIME_USER=""
LAB_ROOT=""
NODE_IP=""
AUTHORIZED_KEY_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runtime-user) RUNTIME_USER="${2:?--runtime-user requires a value}"; shift 2 ;;
    --lab-root) LAB_ROOT="${2:?--lab-root requires a value}"; shift 2 ;;
    --node-ip) NODE_IP="${2:?--node-ip requires a value}"; shift 2 ;;
    --authorized-key-file) AUTHORIZED_KEY_FILE="${2:?--authorized-key-file requires a value}"; shift 2 ;;
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
if [[ ! -s "${AUTHORIZED_KEY_FILE}" ]]; then
  echo "SSH public key was not found: ${AUTHORIZED_KEY_FILE}" >&2
  exit 1
fi
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
  ca-certificates curl gnupg iproute2 iptables lsb-release \
  linux-tools-common linux-tools-generic openjdk-21-jre-headless \
  python3-yaml rsync openssh-server tcpdump tshark libcap2-bin

if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now docker ssh

if ! id "${RUNTIME_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "/var/lib/${RUNTIME_USER}" --shell /bin/bash "${RUNTIME_USER}"
fi
usermod -aG docker "${RUNTIME_USER}"
RUNTIME_HOME="$(getent passwd "${RUNTIME_USER}" | cut -d: -f6)"
RUNTIME_GROUP="$(id -gn "${RUNTIME_USER}")"

install -d -m 0700 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${RUNTIME_HOME}/.ssh"
touch "${RUNTIME_HOME}/.ssh/authorized_keys"
if ! grep -qxF "$(cat "${AUTHORIZED_KEY_FILE}")" "${RUNTIME_HOME}/.ssh/authorized_keys"; then
  cat "${AUTHORIZED_KEY_FILE}" >> "${RUNTIME_HOME}/.ssh/authorized_keys"
fi
chown "${RUNTIME_USER}:${RUNTIME_GROUP}" "${RUNTIME_HOME}/.ssh/authorized_keys"
chmod 0600 "${RUNTIME_HOME}/.ssh/authorized_keys"
loginctl enable-linger "${RUNTIME_USER}" >/dev/null 2>&1 || true

if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --disable traefik --disable servicelb --disable local-storage --disable metrics-server --write-kubeconfig-mode 600 --node-ip ${NODE_IP} --advertise-address ${NODE_IP}" sh -
else
  systemctl enable --now k3s
fi

swapoff -a || true
if [[ -f /swap.img ]]; then
  sed -i '/^[[:space:]]*\/swap.img[[:space:]]/ s/^/# disabled for internal lab: /' /etc/fstab
fi

install -d -m 0755 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${LAB_ROOT}"
for directory in config docker grafana helpers k8s load-test-runtime notify results state thread-stats; do
  install -d -m 0755 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${LAB_ROOT}/${directory}"
done
install -d -m 0755 -o 65534 -g 65534 "${LAB_ROOT}/prometheus"
install -d -m 0755 -o 10001 -g 10001 "${LAB_ROOT}/loki"

install -d -m 0700 -o "${RUNTIME_USER}" -g "${RUNTIME_GROUP}" "${RUNTIME_HOME}/.kube"
sed "s#https://127.0.0.1:6443#https://${NODE_IP}:6443#" /etc/rancher/k3s/k3s.yaml > "${RUNTIME_HOME}/.kube/config"
chown "${RUNTIME_USER}:${RUNTIME_GROUP}" "${RUNTIME_HOME}/.kube/config"
chmod 0600 "${RUNTIME_HOME}/.kube/config"

install -d -m 0755 /etc/ckc-lab /usr/local/libexec/ckc-lab
cat > /etc/ckc-lab/host.env <<EOF
LAB_ROOT=${LAB_ROOT}
EOF
chmod 0644 /etc/ckc-lab/host.env
cat > /usr/local/libexec/ckc-lab/import-k3s-images <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
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
cat > /etc/sudoers.d/ckc-lab <<EOF
${RUNTIME_USER} ALL=(root) NOPASSWD: /usr/local/libexec/ckc-lab/import-k3s-images
EOF
chmod 0440 /etc/sudoers.d/ckc-lab
visudo -cf /etc/sudoers.d/ckc-lab >/dev/null

setcap cap_net_admin,cap_net_raw=eip "$(command -v tcpdump)"

echo "Internal-lab host bootstrap is complete."
echo "  runtime_user=${RUNTIME_USER}"
echo "  runtime_home=${RUNTIME_HOME}"
echo "  lab_root=${LAB_ROOT}"
echo "  node_ip=${NODE_IP}"
