#!/bin/bash
set -euxo pipefail

dnf update -y
dnf install -y docker git unzip tar gzip shadow-utils dnf-plugins-core

systemctl enable --now docker
usermod -aG docker ec2-user

mkdir -p /opt/ckc-runner/prometheus
mkdir -p /opt/ckc-runner/grafana
mkdir -p /opt/ckc-runner/reports
mkdir -p /opt/ckc-runner/repo
chown -R ec2-user:ec2-user /opt/ckc-runner

if ! command -v aws >/dev/null 2>&1; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp
  /tmp/aws/install
fi

rpm --import https://rpm.releases.hashicorp.com/gpg
dnf config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
dnf install -y terraform

curl -fsSL "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3" -o /tmp/get_helm.sh
chmod +x /tmp/get_helm.sh
/tmp/get_helm.sh

curl -fsSL "https://dl.k8s.io/release/v1.33.0/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl

mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

cat > /etc/profile.d/ckc-runner.sh <<'EOF'
export CKC_RUNNER_HOME=/opt/ckc-runner
EOF
