#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../../.." && pwd)"
STATE_DIR="${REPO_ROOT}/.internal-lab"
TEST_STATE_PATH="${STATE_DIR}/selected-test-definition"
TEST_DIR="${REPO_ROOT}/demo/infra/shared/test-definitions"

mkdir -p "${STATE_DIR}"

mapfile -t TESTS < <(find "${TEST_DIR}" -maxdepth 1 -type f -name '*.yaml' | sort)

if [[ "${#TESTS[@]}" -eq 0 ]]; then
  echo "No test definitions found in ${TEST_DIR}." >&2
  exit 1
fi

echo "Available test definitions:"
for index in "${!TESTS[@]}"; do
  printf "  %2d) %s\n" "$((index + 1))" "$(basename "${TESTS[$index]}" .yaml)"
done

echo
read -r -p "Select test definition number: " choice

if ! [[ "${choice}" =~ ^[0-9]+$ ]]; then
  echo "Invalid selection: ${choice}" >&2
  exit 1
fi

selected_index=$((choice - 1))
if (( selected_index < 0 || selected_index >= ${#TESTS[@]} )); then
  echo "Selection out of range: ${choice}" >&2
  exit 1
fi

selected="${TESTS[$selected_index]#"${REPO_ROOT}/"}"
printf "%s\n" "${selected}" > "${TEST_STATE_PATH}"

echo "Selected test definition: $(basename "${selected}" .yaml)"
