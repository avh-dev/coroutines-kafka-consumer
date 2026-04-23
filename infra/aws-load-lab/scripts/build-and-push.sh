#!/usr/bin/env sh

set -eu

REGION="${1:-eu-central-1}"
ENVIRONMENT="${2:-dev}"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

if [ -z "${ACCOUNT_ID}" ]; then
  echo "Unable to resolve AWS account id." >&2
  exit 1
fi

REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
PREFIX="${REGISTRY}/ckc-load-lab-${ENVIRONMENT}"
DEMO_IMAGE="${PREFIX}/demo:latest"
LOAD_TEST_IMAGE="${PREFIX}/load-test:latest"

aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${REGISTRY}"

docker build -f coroutines-kafka-consumer-demo/Dockerfile -t "${DEMO_IMAGE}" .
docker push "${DEMO_IMAGE}"

docker build -f coroutines-kafka-consumer-demo-load-test/Dockerfile -t "${LOAD_TEST_IMAGE}" .
docker push "${LOAD_TEST_IMAGE}"

printf 'Pushed images:\n'
printf '  %s\n' "${DEMO_IMAGE}"
printf '  %s\n' "${LOAD_TEST_IMAGE}"
