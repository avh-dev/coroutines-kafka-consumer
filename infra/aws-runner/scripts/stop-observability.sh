#!/usr/bin/env sh

set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
RUNNER_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${RUNNER_DIR}"
docker compose down
