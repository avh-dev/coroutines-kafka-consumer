#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"

exec python3 "${LAB_ROOT}/helpers/export-result.py" --lab-root "${LAB_ROOT}" "$@"
