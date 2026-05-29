#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-internal-lab}"
TEST_STATE_PATH="${LAB_ROOT}/config/selected-test-definition"
TEST_DIR="${LAB_ROOT}/workspace/demo/infra/shared/test-definitions"

mkdir -p "${LAB_ROOT}/config"

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

selected="demo/infra/shared/test-definitions/$(basename "${TESTS[$selected_index]}")"
printf "%s\n" "${selected}" > "${TEST_STATE_PATH}"

echo "Selected test definition: $(basename "${selected}" .yaml)"
