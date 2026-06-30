#!/usr/bin/env sh

set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root." >&2
  exit 1
fi

if [ -z "${LAB_NODE_IP:-}" ]; then
  echo "LAB_NODE_IP is required." >&2
  exit 1
fi

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release linux-tools-common linux-tools-generic openjdk-21-jre-headless python3-yaml

if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

systemctl enable --now docker

if ! command -v helm >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --disable traefik --disable servicelb --disable local-storage --disable metrics-server --write-kubeconfig-mode 644 --node-ip ${LAB_NODE_IP} --advertise-address ${LAB_NODE_IP}" sh -
else
  systemctl enable --now k3s
fi

swapoff -a || true
if [ -f /swap.img ]; then
  sed -i '/^[[:space:]]*\/swap\.img[[:space:]]/ s/^/# disabled for internal lab: /' /etc/fstab
  rm -f /swap.img
fi

if command -v cpupower >/dev/null 2>&1; then
  cpupower frequency-set -g performance || true
fi

mkdir -p "${LAB_ROOT}"

echo "Server prerequisites are ready."
echo "  lab_root=${LAB_ROOT}"
echo "  lab_node_ip=${LAB_NODE_IP}"
