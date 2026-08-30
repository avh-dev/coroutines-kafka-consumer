#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../../.." && pwd)"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

if [ -z "${ACCOUNT_ID}" ]; then
  echo "Unable to resolve AWS account id." >&2
  exit 1
fi

REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
PREFIX="${REGISTRY}/ckc-load-lab-${ENVIRONMENT}"
DEMO_IMAGE="${PREFIX}/demo:latest"
DEMO_STUBS_IMAGE="${PREFIX}/demo-stubs:latest"
LOAD_TEST_IMAGE="${PREFIX}/load-test:latest"
TARGET_PLATFORM="${CKC_AWS_IMAGE_PLATFORM:-linux/amd64}"

cd "${REPO_DIR}"

./gradlew \
  :ckc-demo:installDist \
  :ckc-demo-stubs:installDist \
  :ckc-demo-load-test:installDist

aws ecr describe-repositories \
  --region "${REGION}" \
  --repository-names \
  "ckc-load-lab-${ENVIRONMENT}/demo" \
  "ckc-load-lab-${ENVIRONMENT}/demo-stubs" \
  "ckc-load-lab-${ENVIRONMENT}/load-test" >/dev/null

aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${REGISTRY}"

docker buildx build --platform "${TARGET_PLATFORM}" --push -f demo/ckc-demo/Dockerfile -t "${DEMO_IMAGE}" demo/ckc-demo

docker buildx build --platform "${TARGET_PLATFORM}" --push -f demo/ckc-demo-stubs/Dockerfile -t "${DEMO_STUBS_IMAGE}" demo/ckc-demo-stubs

docker buildx build --platform "${TARGET_PLATFORM}" --push -f demo/ckc-demo-load-test/Dockerfile -t "${LOAD_TEST_IMAGE}" demo/ckc-demo-load-test

printf 'Pushed images:\n'
printf '  %s\n' "${DEMO_IMAGE}"
printf '  %s\n' "${DEMO_STUBS_IMAGE}"
printf '  %s\n' "${LOAD_TEST_IMAGE}"
