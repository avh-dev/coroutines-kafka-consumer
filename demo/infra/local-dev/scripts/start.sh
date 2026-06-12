#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"

audit_enabled() {
  local answer=""

  case "${LOCAL_DEV_AUDIT:-}" in
    true|TRUE|1|yes|YES|y|Y)
      return 0
      ;;
    false|FALSE|0|no|NO|n|N)
      return 1
      ;;
  esac

  if [[ ! -t 0 ]]; then
    return 1
  fi

  read -r -p "Start local audit collector? [y/N] " answer
  case "${answer}" in
    y|Y|yes|YES)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

compose_args=(-f "${COMPOSE_FILE}")
start_audit=false
if audit_enabled; then
  compose_args+=(--profile audit)
  start_audit=true
fi

if [[ "${start_audit}" == "true" ]]; then
  docker compose "${compose_args[@]}" rm -f -s audit-archiver fluent-bit >/dev/null 2>&1 || true
fi

docker compose "${compose_args[@]}" up -d --remove-orphans "$@"
