#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"

python3 "${LAB_ROOT}/helpers/target-draft.py" --lab-root "${LAB_ROOT}" "$@"
