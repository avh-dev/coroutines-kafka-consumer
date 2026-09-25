#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"

exec python3 "${LAB_ROOT}/helpers/run-experiment.py" "$@"
