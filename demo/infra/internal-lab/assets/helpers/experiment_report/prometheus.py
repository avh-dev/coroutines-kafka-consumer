from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Any


STANDARD_MEASUREMENTS = {
    "latency_p95_ms": (
        "1000 * histogram_quantile(0.95, sum by (le) "
        "(increase(demo_ckc_record_end_to_end_duration_seconds_bucket"
        '{{pod=~"ckc-demo-.+"}}[{window}])))'
    ),
    "latency_p99_ms": (
        "1000 * histogram_quantile(0.99, sum by (le) "
        "(increase(demo_ckc_record_end_to_end_duration_seconds_bucket"
        '{{pod=~"ckc-demo-.+"}}[{window}])))'
    ),
    "freshness_gap_p95_ms": (
        "1000 * histogram_quantile(0.95, sum by (le) "
        "(increase(ckc_demo_cauldron_telemetry_event_gap_seconds_bucket"
        '{{pod=~"ckc-demo-.+"}}[{window}])))'
    ),
    "throughput_average_rps": (
        "sum(increase(demo_ckc_record_process_duration_seconds_count"
        '{{pod=~"ckc-demo-.+"}}[{window}])) / {seconds}'
    ),
    "cpu_average_cores": (
        "sum(increase(container_cpu_usage_seconds_total"
        '{{namespace="ckc-perf", container="demo", pod=~"ckc-demo-.+"}}[{window}])) / {seconds}'
    ),
}


class PrometheusClient:
    def __init__(self, base_url: str, timeout_seconds: int = 15):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def query_scalar(self, query: str, at: datetime) -> float | None:
        params = urllib.parse.urlencode({"query": query, "time": at.timestamp()})
        url = f"{self.base_url}/api/v1/query?{params}"
        with urllib.request.urlopen(url, timeout=self.timeout_seconds) as response:
            document = json.loads(response.read().decode("utf-8"))
        if document.get("status") != "success":
            raise ValueError(f"Prometheus query failed: {document.get('error', 'unknown error')}")
        result = document.get("data", {}).get("result", [])
        values = []
        for series in result:
            value = series.get("value")
            if isinstance(value, list) and len(value) == 2:
                try:
                    parsed = float(value[1])
                except (TypeError, ValueError):
                    continue
                if parsed == parsed and parsed not in (float("inf"), float("-inf")):
                    values.append(parsed)
        return sum(values) if values else None


def collect_standard_measurements(
    client: PrometheusClient,
    start: datetime,
    duration_seconds: float,
) -> dict[str, float | None]:
    seconds = max(1, int(round(duration_seconds)))
    window = f"{seconds}s"
    end = start + timedelta(seconds=seconds)
    measurements: dict[str, float | None] = {}
    for name, template in STANDARD_MEASUREMENTS.items():
        query = template.format(window=window, seconds=seconds)
        measurements[name] = client.query_scalar(query, end)
    return measurements
