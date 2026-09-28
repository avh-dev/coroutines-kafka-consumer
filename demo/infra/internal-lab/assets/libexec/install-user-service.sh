#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
TEMPLATE="${LAB_ROOT}/systemd/ckc-experiment.service.in"
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_PATH="${UNIT_DIR}/ckc-experiment.service"
TELEGRAM_SERVICE_TEMPLATE="${LAB_ROOT}/systemd/ckc-telegram-dispatch.service.in"
TELEGRAM_TIMER_TEMPLATE="${LAB_ROOT}/systemd/ckc-telegram-dispatch.timer.in"
TELEGRAM_SERVICE_PATH="${UNIT_DIR}/ckc-telegram-dispatch.service"
TELEGRAM_TIMER_PATH="${UNIT_DIR}/ckc-telegram-dispatch.timer"

for required_template in "${TEMPLATE}" "${TELEGRAM_SERVICE_TEMPLATE}" "${TELEGRAM_TIMER_TEMPLATE}"; do
  if [[ ! -f "${required_template}" ]]; then
    echo "Managed user service template is missing: ${required_template}" >&2
    exit 1
  fi
done

mkdir -p "${UNIT_DIR}" "${LAB_ROOT}/logs" "${LAB_ROOT}/state/experiment" "${LAB_ROOT}/state/notifications/telegram"
sed "s#@LAB_ROOT@#${LAB_ROOT}#g" "${TEMPLATE}" > "${UNIT_PATH}.tmp"
mv "${UNIT_PATH}.tmp" "${UNIT_PATH}"
sed "s#@LAB_ROOT@#${LAB_ROOT}#g" "${TELEGRAM_SERVICE_TEMPLATE}" > "${TELEGRAM_SERVICE_PATH}.tmp"
mv "${TELEGRAM_SERVICE_PATH}.tmp" "${TELEGRAM_SERVICE_PATH}"
sed "s#@LAB_ROOT@#${LAB_ROOT}#g" "${TELEGRAM_TIMER_TEMPLATE}" > "${TELEGRAM_TIMER_PATH}.tmp"
mv "${TELEGRAM_TIMER_PATH}.tmp" "${TELEGRAM_TIMER_PATH}"
systemctl --user daemon-reload
systemctl --user enable --now ckc-telegram-dispatch.timer >/dev/null
systemctl --user start --no-block ckc-telegram-dispatch.service >/dev/null 2>&1 || true

echo "Managed experiment service installed: ${UNIT_PATH}"
echo "Telegram outbox timer installed: ${TELEGRAM_TIMER_PATH}"
