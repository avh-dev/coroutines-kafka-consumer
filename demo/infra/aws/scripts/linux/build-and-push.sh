#!/usr/bin/env sh

set -eu

REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-dev}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/../../../.." && pwd)"

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

cd "${REPO_DIR}"

./gradlew \
  :ckc-demo:bootJar \
  :ckc-demo-stubs:fatJar \
  :ckc-demo-load-test:fatJar

aws ecr describe-repositories \
  --region "${REGION}" \
  --repository-names \
  "ckc-load-lab-${ENVIRONMENT}/demo" \
  "ckc-load-lab-${ENVIRONMENT}/demo-stubs" \
  "ckc-load-lab-${ENVIRONMENT}/load-test" >/dev/null

aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${REGISTRY}"

docker build -f demo/ckc-demo/Dockerfile -t "${DEMO_IMAGE}" demo/ckc-demo
docker push "${DEMO_IMAGE}"

docker build -f demo/ckc-demo-stubs/Dockerfile -t "${DEMO_STUBS_IMAGE}" demo/ckc-demo-stubs
docker push "${DEMO_STUBS_IMAGE}"

docker build -f demo/ckc-demo-load-test/Dockerfile -t "${LOAD_TEST_IMAGE}" demo/ckc-demo-load-test
docker push "${LOAD_TEST_IMAGE}"

printf 'Pushed images:\n'
printf '  %s\n' "${DEMO_IMAGE}"
printf '  %s\n' "${DEMO_STUBS_IMAGE}"
printf '  %s\n' "${LOAD_TEST_IMAGE}"
