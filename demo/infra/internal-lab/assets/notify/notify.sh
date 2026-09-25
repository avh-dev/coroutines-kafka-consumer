#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
TELEGRAM_ENV="${LAB_ROOT}/config/telegram.env"

if [[ ! -r "${TELEGRAM_ENV}" ]]; then
  exit 0
fi

# shellcheck disable=SC1090
source "${TELEGRAM_ENV}"
exec "${LAB_ROOT}/notify/notify-telegram.py" "$@"
