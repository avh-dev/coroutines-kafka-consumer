#!/usr/bin/env bash

set -euo pipefail

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
INTERNAL_EXPERIMENTS_ROOT="${INTERNAL_EXPERIMENTS_ROOT:-${LAB_ROOT}/results/experiments}"
REPOSITORY_ENV="${LAB_ROOT}/config/repository.env"
if [[ -z "${CKC_REPOSITORY_ROOT:-}" && -f "${REPOSITORY_ENV}" ]]; then
  # shellcheck disable=SC1090
  source "${REPOSITORY_ENV}"
fi
AWS_EXPERIMENTS_ROOT="${AWS_EXPERIMENTS_ROOT:-${CKC_REPOSITORY_ROOT:+${CKC_REPOSITORY_ROOT}/.demo-infra/experiments/aws}}"
EXPORTS_ROOT="${EXPORTS_ROOT:-${LAB_ROOT}/results/exports}"
ARCHIVE="${EXPORTS_ROOT}/latest-report.zip"

latest_report_root=""
latest_report_timestamp=-1
latest_report_source=""

consider_report_root() {
  local summary_file="$1"
  local candidate_report_root="$2"
  local source="$3"
  local report_file timestamp candidate_timestamp
  local -a candidate_report_files=()

  [[ -f "${summary_file}" && -d "${candidate_report_root}" ]] || return 0
  mapfile -d '' candidate_report_files < <(
    find "${candidate_report_root}" -mindepth 2 -type f -name report.md -print0 | sort -z
  )
  (( ${#candidate_report_files[@]} > 0 )) || return 0

  timestamp=-1
  for report_file in "${candidate_report_files[@]}"; do
    candidate_timestamp="$(stat -c '%Y' "${report_file}")"
    if (( candidate_timestamp > timestamp )); then
      timestamp="${candidate_timestamp}"
    fi
  done
  if (( timestamp > latest_report_timestamp )); then
    latest_report_timestamp="${timestamp}"
    latest_report_root="${candidate_report_root}"
    latest_report_source="${source}"
  fi
}

if [[ -d "${INTERNAL_EXPERIMENTS_ROOT}" ]]; then
  while IFS= read -r -d '' candidate; do
    consider_report_root "${candidate}/summary.json" "${candidate}/reports" "internal-lab:${candidate}"
  done < <(find "${INTERNAL_EXPERIMENTS_ROOT}" -mindepth 1 -maxdepth 1 -type d -print0)
fi

if [[ -n "${AWS_EXPERIMENTS_ROOT}" && -d "${AWS_EXPERIMENTS_ROOT}" ]]; then
  while IFS= read -r -d '' candidate; do
    consider_report_root "${candidate}/result/summary.json" "${candidate}/result/reports" "aws:${candidate}"
  done < <(find "${AWS_EXPERIMENTS_ROOT}" -mindepth 1 -maxdepth 1 -type d -print0)
fi

if [[ -z "${latest_report_root}" ]]; then
  echo "No completed report was found under internal-lab or AWS experiment results." >&2
  exit 1
fi

report_root="${latest_report_root}"
mapfile -d '' report_files < <(find "${report_root}" -mindepth 2 -type f -name report.md -print0 | sort -z)

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
printf 'source=%s\n' "${latest_report_source}" >&2
printf '%s\n' "${ARCHIVE}"
