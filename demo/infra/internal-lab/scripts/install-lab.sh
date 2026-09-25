#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

echo "install-lab.sh is deprecated; using the configurable lab setup workflow." >&2
exec "${SCRIPT_DIR}/lab.sh" setup "$@"
