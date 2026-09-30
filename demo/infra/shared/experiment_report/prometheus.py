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
        "avg_over_time((sum(rate(container_cpu_usage_seconds_total"
        '{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-.+"}}[1m])))[{window}:15s])'
    ),
    "application_memory_average_mib": (
        "avg_over_time((sum(container_memory_working_set_bytes"
        '{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-.+"}}))[{window}:15s]) / 1024 / 1024'
    ),
    "application_network_receive_average_mib_per_second": (
        "avg_over_time((sum(rate(container_network_receive_bytes_total"
        '{{namespace=~"ckc-perf|ckc-app", pod=~"ckc-demo-.+"}}[1m])))[{window}:15s]) / 1024 / 1024'
    ),
    "application_network_transmit_average_mib_per_second": (
        "avg_over_time((sum(rate(container_network_transmit_bytes_total"
        '{{namespace=~"ckc-perf|ckc-app", pod=~"ckc-demo-.+"}}[1m])))[{window}:15s]) / 1024 / 1024'
    ),
    "load_test_cpu_average_cores": (
        "avg_over_time((sum(rate(container_cpu_usage_seconds_total"
        '{{namespace=~"ckc-perf|ckc-loadtest", container="load-test", pod=~"ckc-load-test-.+"}}[1m])))[{window}:15s])'
    ),
    "load_test_memory_average_mib": (
        "avg_over_time((sum(container_memory_working_set_bytes"
        '{{namespace=~"ckc-perf|ckc-loadtest", container="load-test", pod=~"ckc-load-test-.+"}}))[{window}:15s]) / 1024 / 1024'
    ),
    "load_test_network_transmit_average_mib_per_second": (
        "avg_over_time((sum(rate(container_network_transmit_bytes_total"
        '{{namespace=~"ckc-perf|ckc-loadtest", pod=~"ckc-load-test-.+"}}[1m])))[{window}:15s]) / 1024 / 1024'
    ),
    "stubs_cpu_average_cores": (
        "avg_over_time((sum(rate(container_cpu_usage_seconds_total"
        '{{namespace=~"ckc-perf|ckc-app", container="demo-stubs", pod=~"ckc-demo-stubs-.+"}}[1m])))[{window}:15s])'
    ),
    "stubs_memory_average_mib": (
        "avg_over_time((sum(container_memory_working_set_bytes"
        '{{namespace=~"ckc-perf|ckc-app", container="demo-stubs", pod=~"ckc-demo-stubs-.+"}}))[{window}:15s]) / 1024 / 1024'
    ),
    "stubs_network_receive_average_mib_per_second": (
        "avg_over_time((sum(rate(container_network_receive_bytes_total"
        '{{namespace=~"ckc-perf|ckc-app", pod=~"ckc-demo-stubs-.+"}}[1m])))[{window}:15s]) / 1024 / 1024'
    ),
    "broker_cpu_average_cores": (
        "sum(increase(namedprocess_namegroup_cpu_seconds_total"
        '{{job="ckc-host-process-exporter", groupname=~"redpanda|apache-kafka"}}[{window}])) / {seconds}'
    ),
    "broker_memory_average_mib": (
        "sum(avg_over_time(namedprocess_namegroup_memory_bytes"
        '{{job="ckc-host-process-exporter", groupname=~"redpanda|apache-kafka", memtype="resident"}}[{window}])) / 1024 / 1024'
    ),
    "producer_cpu_average_cores": (
        "sum(increase(namedprocess_namegroup_cpu_seconds_total"
        '{{job="ckc-host-process-exporter", groupname="ckc-load-test"}}[{window}])) / {seconds}'
    ),
    "producer_memory_average_mib": (
        "sum(avg_over_time(namedprocess_namegroup_memory_bytes"
        '{{job="ckc-host-process-exporter", groupname="ckc-load-test", memtype="resident"}}[{window}])) / 1024 / 1024'
    ),
    "producer_buffer_utilization_max_percent": (
        "100 * max(max_over_time((1 - ("
        'kafka_producer_buffer_available_bytes{{job="ckc-load-test"}} / '
        'clamp_min(kafka_producer_buffer_total_bytes{{job="ckc-load-test"}}, 1)'
        "))[{window}:]))"
    ),
    "telemetry_poll_batch_average_records": (
        "sum(increase(demo_ckc_poll_records_sum"
        '{{consumer_id="cauldron_events", pod=~"ckc-demo-.+"}}[{window}])) / '
        "clamp_min(sum(increase(demo_ckc_poll_records_count"
        '{{consumer_id="cauldron_events", pod=~"ckc-demo-.+"}}[{window}])), 1)'
    ),
    "telemetry_poll_batch_max_records": (
        "max(max_over_time(demo_ckc_poll_records_max"
        '{{consumer_id="cauldron_events", pod=~"ckc-demo-.+"}}[{window}]))'
    ),
    "telemetry_active_workers_average": (
        "avg(avg_over_time(demo_ckc_workers_active"
        '{{consumer_id="cauldron_events", pod=~"ckc-demo-.+"}}[{window}]))'
    ),
    "telemetry_active_workers_max": (
        "max(max_over_time(demo_ckc_workers_active"
        '{{consumer_id="cauldron_events", pod=~"ckc-demo-.+"}}[{window}]))'
    ),
    "processing_worker_cpu_average_cores": (
        "sum(increase(thread_stats_cpu_seconds_total"
        '{{job="ckc-demo", category=~"^([0-9]+\\\\. )?business$", pod=~"ckc-demo-.+"}}[{window}])) / {seconds}'
    ),
    "processing_worker_allocation_average_bytes_per_second": (
        "sum(increase(thread_stats_allocated_bytes_total"
        '{{job="ckc-demo", category=~"^([0-9]+\\\\. )?business$", pod=~"ckc-demo-.+"}}[{window}])) / {seconds}'
    ),
    "context_switches_average_per_second": (
        "sum(increase(thread_stats_context_switches_total"
        '{{job="ckc-demo", pod=~"ckc-demo-.+"}}[{window}])) / {seconds}'
    ),
    "msk_cpu_average_percent": (
        "max(avg_over_time((aws_kafka_cpu_user_average + "
        "aws_kafka_cpu_system_average)[{window}:60s]))"
    ),
    "msk_cpu_max_percent": (
        "max(max_over_time((aws_kafka_cpu_user_maximum + "
        "aws_kafka_cpu_system_maximum)[{window}:60s]))"
    ),
    "msk_network_processor_utilization_max_percent": (
        "clamp(100 * (1 - min(min_over_time("
        "aws_kafka_network_processor_avg_idle_percent_minimum[{window}]))), 0, 100)"
    ),
    "msk_request_handler_utilization_max_percent": (
        "clamp(100 * (1 - min(min_over_time("
        "aws_kafka_request_handler_avg_idle_percent_minimum[{window}]))), 0, 100)"
    ),
    "msk_ingress_average_mib_per_second": (
        "avg_over_time((sum(aws_kafka_bytes_in_per_sec_average))[{window}:60s]) / 1024 / 1024"
    ),
    "msk_egress_average_mib_per_second": (
        "avg_over_time((sum(aws_kafka_bytes_out_per_sec_average))[{window}:60s]) / 1024 / 1024"
    ),
    "msk_produce_latency_max_ms": (
        "max(max_over_time(aws_kafka_produce_total_time_ms_mean_maximum[{window}]))"
    ),
    "msk_fetch_latency_max_ms": (
        "max(max_over_time(aws_kafka_fetch_consumer_total_time_ms_mean_maximum[{window}]))"
    ),
    "msk_produce_throttle_max_ms": (
        "max(max_over_time(aws_kafka_produce_throttle_time_maximum[{window}]))"
    ),
    "msk_fetch_throttle_max_ms": (
        "max(max_over_time(aws_kafka_fetch_throttle_time_maximum[{window}]))"
    ),
    "msk_storage_io_average_mib_per_second": (
        "avg_over_time((sum(aws_kafka_volume_read_bytes_sum + "
        "aws_kafka_volume_write_bytes_sum) / 60)[{window}:60s]) / 1024 / 1024"
    ),
    "msk_disk_used_max_percent": (
        "max(max_over_time(aws_kafka_kafka_data_logs_disk_used_maximum[{window}]))"
    ),
    "msk_cpu_credit_balance_min": (
        "min(min_over_time(aws_kafka_cpu_credit_balance_minimum[{window}]))"
    ),
    "msk_under_replicated_partitions_max": (
        "max(max_over_time(aws_kafka_under_replicated_partitions_maximum[{window}]))"
    ),
    "msk_offline_partitions_max": (
        "max(max_over_time(aws_kafka_offline_partitions_count_maximum[{window}]))"
    ),
    "redis_engine_cpu_max_percent": (
        "max(max_over_time(aws_elasticache_engine_cpu_utilization_maximum[{window}]))"
    ),
    "redis_network_receive_average_mib_per_second": (
        "avg_over_time((sum(aws_elasticache_network_bytes_in_sum) / 60)[{window}:60s]) / 1024 / 1024"
    ),
    "redis_network_transmit_average_mib_per_second": (
        "avg_over_time((sum(aws_elasticache_network_bytes_out_sum) / 60)[{window}:60s]) / 1024 / 1024"
    ),
    "redis_connections_max": (
        "max(max_over_time(aws_elasticache_curr_connections_maximum[{window}]))"
    ),
    "redis_evictions_total": (
        "sum(sum_over_time(aws_elasticache_evictions_sum[{window}]))"
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
