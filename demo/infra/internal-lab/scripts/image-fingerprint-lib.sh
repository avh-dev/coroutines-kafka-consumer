#!/usr/bin/env bash

image_fingerprint() {
  if ! command -v sha256sum >/dev/null 2>&1; then
    echo "sha256sum is required to calculate the lab image fingerprint." >&2
    exit 1
  fi

  (
    cd "${REPO_ROOT}"
    {
      printf '%s\n' "internal-lab-image-fingerprint-v1"
      for path in \
        settings.gradle.kts \
        build.gradle.kts \
        gradle.properties \
        gradle/wrapper/gradle-wrapper.properties \
        ckc-core \
        ckc-micrometer \
        demo/ckc-demo-contracts \
        demo/ckc-demo \
        demo/ckc-demo-stubs
      do
        if [[ -f "${path}" ]]; then
          sha256sum "${path}"
        elif [[ -d "${path}" ]]; then
          find "${path}" \
            -type f \
            ! -path '*/build/*' \
            ! -path '*/.gradle/*' \
            -print0 \
            | sort -z \
            | xargs -0 sha256sum
        fi
      done
    } | sha256sum | awk '{ print $1 }'
  )
}
