#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
# shellcheck disable=SC1090
source "${LAB_ROOT}/config/lab.env"

sudo -n /usr/local/libexec/ckc-lab/cpu-performance-acquire
if [[ -n "${LAB_APPLICATION_TARGET:-}" ]]; then
  if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
    "${LAB_APPLICATION_TARGET}" sudo -n /usr/local/libexec/ckc-lab/cpu-performance-acquire; then
    sudo -n /usr/local/libexec/ckc-lab/cpu-performance-release || true
    exit 1
  fi
fi
