#!/usr/bin/env bash

set -euo pipefail

PHASE_FILE="${CKC_RUN_PHASE_FILE:?CKC_RUN_PHASE_FILE is required}"
PHASE_URI="${CKC_RUN_PHASE_S3_URI:?CKC_RUN_PHASE_S3_URI is required}"
REGION="${CKC_RUN_PHASE_REGION:?CKC_RUN_PHASE_REGION is required}"

aws s3 cp "${PHASE_FILE}" "${PHASE_URI}" --region "${REGION}" --only-show-errors
