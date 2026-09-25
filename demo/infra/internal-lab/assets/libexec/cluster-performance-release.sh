#!/usr/bin/env bash

set -u

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
# shellcheck disable=SC1090
source "${LAB_ROOT}/config/lab.env"

status=0
if [[ -n "${LAB_APPLICATION_TARGET:-}" ]]; then
  ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
    "${LAB_APPLICATION_TARGET}" sudo -n /usr/local/libexec/ckc-lab/cpu-performance-release || status=$?
fi
sudo -n /usr/local/libexec/ckc-lab/cpu-performance-release || status=$?
exit "${status}"
