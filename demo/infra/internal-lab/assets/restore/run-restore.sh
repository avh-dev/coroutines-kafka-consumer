#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
LOKI_PORT="${LOKI_PORT:-3100}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
RESTORE_WORK_DIR="${RESTORE_WORK_DIR:-${SCRIPT_DIR}/.runtime}"

dashboard_url() {
  python3 - "${SCRIPT_DIR}/../manifest.json" "${GRAFANA_PORT}" <<'PY'
import json
import sys
import urllib.parse
from datetime import datetime, timezone

manifest_path = sys.argv[1]
grafana_port = sys.argv[2]


def parse_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def millis(value):
    return str(int(value.timestamp() * 1000))


with open(manifest_path, encoding="utf-8") as file:
    manifest = json.load(file)

metrics = manifest.get("metrics") or {}
start = parse_time(metrics.get("start"))
end = parse_time(metrics.get("end"))

if start is None or end is None:
    starts = []
    ends = []
    for entry in manifest.get("loki", []):
        start_value = parse_time(entry.get("start"))
        end_value = parse_time(entry.get("end"))
        if start_value:
            starts.append(start_value)
        if end_value:
            ends.append(end_value)
    start = min(starts) if starts else None
    end = max(ends) if ends else None

params = {
    "orgId": "1",
}
if start and end:
    params["from"] = millis(start)
    params["to"] = millis(end)
    params["timezone"] = "utc"
params["var-pod"] = ".*"
params["var-pod_grouping"] = "pod"
params["var-event_type_grouping"] = "breakdown"

query = urllib.parse.urlencode(params)
print(f"http://localhost:{grafana_port}/d/ckc-overview/ckc-overview?{query}")
if start and end:
    print(f"{start.astimezone(timezone.utc).isoformat()} .. {end.astimezone(timezone.utc).isoformat()}")
PY
}

cleanup() {
  docker compose -f "${SCRIPT_DIR}/docker-compose.yml" down -v >/dev/null 2>&1 || true
  rm -rf "${RESTORE_WORK_DIR}"
}

wait_for_quit() {
  if [ -t 0 ]; then
    echo "Press q to stop and remove the restore stack."
    while true; do
      key=""
      if IFS= read -r -s -n 1 key < /dev/tty; then
        if [ "${key}" = "q" ] || [ "${key}" = "Q" ]; then
          break
        fi
      fi
    done
  else
    echo "No interactive input is attached; press Ctrl+C to stop and remove the restore stack."
    while true; do
      sleep 3600
    done
  fi
}

trap cleanup EXIT INT TERM

"${SCRIPT_DIR}/import-prometheus.sh"

GRAFANA_PORT="${GRAFANA_PORT}" \
LOKI_PORT="${LOKI_PORT}" \
PROMETHEUS_PORT="${PROMETHEUS_PORT}" \
RESTORE_WORK_DIR="${RESTORE_WORK_DIR}" \
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d --wait

if compgen -G "${SCRIPT_DIR}/../loki/*.jsonl" >/dev/null; then
  "${SCRIPT_DIR}/import-loki.sh" --loki-url "http://127.0.0.1:${LOKI_PORT}" "${SCRIPT_DIR}/../loki/"*.jsonl
else
  echo "No Loki JSONL files found under ${SCRIPT_DIR}/../loki; skipping log import."
fi

dashboard_output="$(dashboard_url)"
dashboard_link="$(printf '%s\n' "${dashboard_output}" | sed -n '1p')"
dashboard_range="$(printf '%s\n' "${dashboard_output}" | sed -n '2p')"
echo
echo "Dashboard:  ${dashboard_link}"
if [ -n "${dashboard_range}" ]; then
  echo "Time range: ${dashboard_range}"
fi
echo "Grafana:    http://localhost:${GRAFANA_PORT}"
echo "Prometheus: http://localhost:${PROMETHEUS_PORT}"
echo "Loki:       http://localhost:${LOKI_PORT}"
echo
wait_for_quit
