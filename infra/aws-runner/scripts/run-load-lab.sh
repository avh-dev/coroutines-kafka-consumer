#!/usr/bin/env sh

set -eu

REGION="${1:-eu-central-1}"
ENVIRONMENT="${2:-dev}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../.." && pwd)"

cd "${REPO_DIR}/infra/aws-load-lab/terraform"

if [ ! -f terraform.tfvars ]; then
  cp terraform.tfvars.example terraform.tfvars
fi

terraform init
terraform apply -auto-approve -var="aws_region=${REGION}" -var="environment=${ENVIRONMENT}"

cd "${REPO_DIR}"
./infra/aws-load-lab/scripts/build-and-push.sh "${REGION}" "${ENVIRONMENT}"
./infra/aws-load-lab/scripts/deploy-lab.sh "${REGION}" "${ENVIRONMENT}"

printf 'Load lab base infrastructure and namespaces are ready.\n'
