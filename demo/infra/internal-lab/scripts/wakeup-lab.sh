#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "${SCRIPT_DIR}/helpers/wakeup-lab.py" "$@"
fi

if command -v python >/dev/null 2>&1; then
  exec python "${SCRIPT_DIR}/helpers/wakeup-lab.py" "$@"
fi

if command -v py >/dev/null 2>&1; then
  exec py -3 "${SCRIPT_DIR}/helpers/wakeup-lab.py" "$@"
fi

echo "Python 3 was not found. Install Python 3 or add it to PATH." >&2
exit 1
