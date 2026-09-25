#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
TEMPLATE="${LAB_ROOT}/systemd/ckc-experiment.service.in"
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_PATH="${UNIT_DIR}/ckc-experiment.service"

if [[ ! -f "${TEMPLATE}" ]]; then
  echo "Managed experiment service template is missing: ${TEMPLATE}" >&2
  exit 1
fi

mkdir -p "${UNIT_DIR}" "${LAB_ROOT}/logs" "${LAB_ROOT}/state/experiment"
sed "s#@LAB_ROOT@#${LAB_ROOT}#g" "${TEMPLATE}" > "${UNIT_PATH}.tmp"
mv "${UNIT_PATH}.tmp" "${UNIT_PATH}"
systemctl --user daemon-reload

echo "Managed experiment service installed: ${UNIT_PATH}"
