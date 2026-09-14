#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
EXPERIMENTS_ROOT="${LAB_ROOT}/results/experiments"
EXPORTS_ROOT="${LAB_ROOT}/results/exports"
ARCHIVE="${EXPORTS_ROOT}/latest-report.zip"

if [[ ! -d "${EXPERIMENTS_ROOT}" ]]; then
  echo "Experiment results directory was not found: ${EXPERIMENTS_ROOT}" >&2
  exit 1
fi

latest_experiment=""
while IFS= read -r experiment_id; do
  candidate="${EXPERIMENTS_ROOT}/${experiment_id}"
  [[ -f "${candidate}/summary.json" ]] || continue
  latest_experiment="${candidate}"
  break
done < <(find "${EXPERIMENTS_ROOT}" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | LC_ALL=C sort -r)

if [[ -z "${latest_experiment}" ]]; then
  echo "No experiment summary was found under ${EXPERIMENTS_ROOT}" >&2
  exit 1
fi

report_root="${latest_experiment}/reports"
if [[ ! -d "${report_root}" ]]; then
  echo "Latest experiment report is not ready: ${latest_experiment}" >&2
  exit 1
fi

mapfile -d '' report_files < <(find "${report_root}" -mindepth 2 -type f -name report.md -print0 | sort -z)
if (( ${#report_files[@]} == 0 )); then
  echo "Latest experiment report is not ready: ${latest_experiment}" >&2
  exit 1
fi

mkdir -p "${EXPORTS_ROOT}"
temporary_dir="$(mktemp -d "${EXPORTS_ROOT}/.latest-report.XXXXXX")"
temporary_archive="${temporary_dir}/latest-report.zip"
trap 'rm -rf -- "${temporary_dir}"' EXIT

if (( ${#report_files[@]} == 1 )); then
  report_dir="$(dirname -- "${report_files[0]}")"
  mapfile -d '' archive_files < <(
    cd "${report_dir}"
    find . -type f \( -name report.md -o -name '*.svg' \) -print0 | sort -z
  )
  (
    cd "${report_dir}"
    zip -q "${temporary_archive}" "${archive_files[@]}"
  )
else
  archive_files=()
  for report_file in "${report_files[@]}"; do
    report_dir="$(dirname -- "${report_file}")"
    while IFS= read -r -d '' asset; do
      archive_files+=("${asset#"${report_root}/"}")
    done < <(find "${report_dir}" -type f \( -name report.md -o -name '*.svg' \) -print0 | sort -z)
  done
  (
    cd "${report_root}"
    zip -q "${temporary_archive}" "${archive_files[@]}"
  )
fi

chmod 0644 "${temporary_archive}"
mv -f -- "${temporary_archive}" "${ARCHIVE}"
rmdir -- "${temporary_dir}"
trap - EXIT
printf '%s\n' "${ARCHIVE}"
