from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Any


STANDARD_MEASUREMENTS = {
    "kafka_network_receive_bytes_per_message": (
        "sum(increase(ckc_kafka_client_network_bytes_total{{direction=\"receive\"}}[{window}])) / "
        "clamp_min(sum(increase(demo_ckc_record_process_duration_seconds_count[{window}])), 1)"
    ),
    "kafka_network_transmit_bytes_per_message": (
        "sum(increase(ckc_kafka_client_network_bytes_total{{direction=\"transmit\"}}[{window}])) / "
        "clamp_min(sum(increase(demo_ckc_record_process_duration_seconds_count[{window}])), 1)"
    ),
    "kafka_network_counter_read_success": "min_over_time(ckc_kafka_network_counter_read_success[{window}])",
    "kafka_network_rules_present": "min_over_time(ckc_kafka_network_rules_present[{window}])",
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
    "processing_duration_average_ms": (
        "1000 * sum(increase(demo_ckc_record_process_duration_seconds_sum"
        '{{pod=~"ckc-demo-.+"}}[{window}])) / clamp_min(sum(increase('
        "demo_ckc_record_process_duration_seconds_count"
        '{{pod=~"ckc-demo-.+"}}[{window}])), 1)'
    ),
    "consumer_lag_max_records": (
        "max(max_over_time(kafka_consumergroup_lag[{window}]))"
    ),
    "work_queue_utilization_max_percent": (
        "100 * max(max_over_time((sum(demo_ckc_work_queue_size"
        '{{pod=~"ckc-demo-.+"}}) / clamp_min(sum(demo_ckc_work_queue_capacity'
        '{{pod=~"ckc-demo-.+"}}), 1))[{window}:15s]))'
    ),
    "worker_utilization_max_percent": (
        "100 * max(max_over_time((sum(demo_ckc_workers_active"
        '{{pod=~"ckc-demo-.+"}}) / clamp_min(sum(demo_ckc_workers'
        '{{pod=~"ckc-demo-.+"}}), 1))[{window}:15s]))'
    ),
    "cpu_throttling_seconds_total": (
        "sum(increase(container_cpu_cfs_throttled_seconds_total"
        '{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-.+"}}[{window}]))'
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
    "application_replicas_average": (
        "avg_over_time((count(last_over_time(container_memory_working_set_bytes"
        '{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-.+"}}[1m])))[{window}:15s])'
    ),
    "application_replicas_min": (
        "min_over_time((count(last_over_time(container_memory_working_set_bytes"
        '{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-.+"}}[1m])))[{window}:15s])'
    ),
    "application_replicas_max": (
        "max_over_time((count(last_over_time(container_memory_working_set_bytes"
        '{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-.+"}}[1m])))[{window}:15s])'
    ),
    "application_nodes_average": (
        "avg_over_time((count(machine_cpu_cores"
        '{{job="kubernetes-cadvisor", node_role="application"}}))[{window}:15s])'
    ),
    "application_nodes_min": (
        "min_over_time((count(machine_cpu_cores"
        '{{job="kubernetes-cadvisor", node_role="application"}}))[{window}:15s])'
    ),
    "application_nodes_max": (
        "max_over_time((count(machine_cpu_cores"
        '{{job="kubernetes-cadvisor", node_role="application"}}))[{window}:15s])'
    ),
    "application_node_hours": (
        "avg_over_time((count(machine_cpu_cores"
        '{{job="kubernetes-cadvisor", node_role="application"}}))[{window}:15s]) * {seconds} / 3600'
    ),
    "support_node_hours": (
        "avg_over_time((count(machine_cpu_cores"
        '{{job="kubernetes-cadvisor", node_role="support"}}))[{window}:15s]) * {seconds} / 3600'
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
    "producer_records_sent_total": (
        "sum(increase(ckc_load_test_producer_records_sent_total"
        '{{job="ckc-load-test", pod=~"ckc-load-test-.+"}}[{window}]))'
    ),
    "producer_records_acked_total": (
        "sum(increase(ckc_load_test_producer_records_acked_total"
        '{{job="ckc-load-test", pod=~"ckc-load-test-.+"}}[{window}]))'
    ),
    "producer_records_failed_total": (
        "sum(increase(ckc_load_test_producer_records_failed_total"
        '{{job="ckc-load-test", pod=~"ckc-load-test-.+"}}[{window}]))'
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
    "voluntary_context_switches_average_per_second": (
        "sum(increase(thread_stats_context_switches_total"
        '{{job="ckc-demo", pod=~"ckc-demo-.+", type="voluntary"}}[{window}])) / {seconds}'
    ),
    "involuntary_context_switches_average_per_second": (
        "sum(increase(thread_stats_context_switches_total"
        '{{job="ckc-demo", pod=~"ckc-demo-.+", type="involuntary"}}[{window}])) / {seconds}'
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
        "min(min_over_time(aws_kafka_cpucredit_balance_minimum[{window}]))"
    ),
    "msk_under_replicated_partitions_max": (
        "max(max_over_time(aws_kafka_under_replicated_partitions_maximum[{window}]))"
    ),
    "msk_offline_partitions_max": (
        "max(max_over_time(aws_kafka_offline_partitions_count_maximum[{window}]))"
    ),
    "redis_engine_cpu_max_percent": (
        "max(max_over_time(aws_elasticache_engine_cpuutilization_maximum[{window}]))"
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

REPORT_TIME_SERIES = {
    "published_rps": 'sum(rate(ckc_load_test_producer_records_acked_total{job="ckc-load-test"}[1m]))',
    "processed_rps": 'sum(rate(demo_ckc_record_process_duration_seconds_count{pod=~"ckc-demo-.+"}[1m]))',
    "latency_p99_ms": (
        '1000 * histogram_quantile(0.99, sum by (le) (rate('
        'demo_ckc_record_end_to_end_duration_seconds_bucket{pod=~"ckc-demo-.+"}[1m])))'
    ),
    "consumer_lag_records": "max(kafka_consumergroup_lag)",
    "queue_utilization_percent": (
        '100 * sum(demo_ckc_work_queue_size{pod=~"ckc-demo-.+"}) / '
        'clamp_min(sum(demo_ckc_work_queue_capacity{pod=~"ckc-demo-.+"}), 1)'
    ),
    "application_replicas": (
        'count(last_over_time(container_memory_working_set_bytes{namespace=~"ckc-perf|ckc-app", '
        'container="demo", pod=~"ckc-demo-.+"}[1m]))'
    ),
}

for _workload in ("order", "batch", "telemetry"):
    for _aggregate in ("average", "min", "max"):
        _function = {"average": "avg_over_time", "min": "min_over_time", "max": "max_over_time"}[_aggregate]
        STANDARD_MEASUREMENTS[f"application_{_workload}_replicas_{_aggregate}"] = (
            f'{_function}((count(last_over_time(container_memory_working_set_bytes'
            f'{{{{namespace=~"ckc-perf|ckc-app", container="demo", pod=~"ckc-demo-{_workload}-.+"}}}}[1m]))'
            ')[{window}:15s])'
        )


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

    def query_range(self, query: str, start: datetime, end: datetime, step_seconds: int) -> list[tuple[float, float]]:
        params = urllib.parse.urlencode({
            "query": query,
            "start": start.timestamp(),
            "end": end.timestamp(),
            "step": max(1, step_seconds),
        })
        url = f"{self.base_url}/api/v1/query_range?{params}"
        with urllib.request.urlopen(url, timeout=self.timeout_seconds) as response:
            document = json.loads(response.read().decode("utf-8"))
        if document.get("status") != "success":
            raise ValueError(f"Prometheus range query failed: {document.get('error', 'unknown error')}")
        points: dict[float, float] = {}
        for series in document.get("data", {}).get("result", []):
            for value in series.get("values", []):
                if not isinstance(value, list) or len(value) != 2:
                    continue
                try:
                    timestamp, parsed = float(value[0]), float(value[1])
                except (TypeError, ValueError):
                    continue
                if parsed == parsed and parsed not in (float("inf"), float("-inf")):
                    points[timestamp] = points.get(timestamp, 0.0) + parsed
        return sorted(points.items())


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


def collect_kafka_network_counters(
    client: PrometheusClient,
    start: datetime,
    duration_seconds: float,
    run_id: str,
    implementation: str | None = None,
) -> dict[str, Any]:
    """Read sidecar counters for one run without relying on legacy snapshots."""
    seconds = max(1, int(round(duration_seconds)))
    end = start + timedelta(seconds=seconds)
    selector = f'run_id="{run_id}"'
    def value(metric: str, role: str, direction: str | None = None) -> float | None:
        labels = f'{selector},role="{role}"' + (f',direction="{direction}"' if direction else "")
        return client.query_scalar(f"sum(increase({metric}{{{labels}}}[{seconds}s]))", end)
    def gauge(metric: str) -> float | None:
        return client.query_scalar(f"min(min_over_time({metric}{{{selector}}}[{seconds}s]))", end)
    rules, reads = gauge("ckc_kafka_network_rules_present"), gauge("ckc_kafka_network_counter_read_success")
    if rules is None and reads is None:
        return {"status": "unavailable", "source": "nftables via Prometheus"}
    result: dict[str, Any] = {"status": "available" if rules == 1 and reads == 1 else "unavailable", "source": "nftables via Prometheus", "rules_present": rules, "read_success": reads}
    def denominator(role: str) -> tuple[float | None, str]:
        if role == "producer":
            query = f'sum(increase(ckc_load_test_producer_records_acked_total{{run_id="{run_id}"}}[{seconds}s]))'
            source = "acknowledged_published"
        elif implementation in {"ckc", "ckc-sync"}:
            # CKC exposes the exact count returned from Kafka poll, which is
            # the closest denominator for consumer-side Kafka network traffic.
            query = f'sum(increase(demo_ckc_poll_records_sum{{pod=~"ckc-demo-.+"}}[{seconds}s]))'
            source = "ckc_polled"
        else:
            # Spring-based implementations do not expose CKC's poll summary.
            # Count both terminal outcomes; a drop is still a record received
            # from Kafka and must not inflate bytes per record.
            query = (
                f'(sum(increase(demo_ckc_record_process_duration_seconds_count{{pod=~"ckc-demo-.+"}}[{seconds}s])) or vector(0)) + '
                f'(sum(increase(demo_ckc_record_dropped_total{{pod=~"ckc-demo-.+"}}[{seconds}s])) or vector(0))'
            )
            source = "processed_plus_dropped"
        return client.query_scalar(query, end), source
    total_rx = total_tx = total_rx_packets = total_tx_packets = 0.0
    for role, result_key in (("producer", "published_messages"), ("consumer", "consumer_records")):
        rx = value("ckc_kafka_client_network_bytes_total", role, "receive") or 0.0
        tx = value("ckc_kafka_client_network_bytes_total", role, "transmit") or 0.0
        rx_packets = value("ckc_kafka_client_network_packets_total", role, "receive") or 0.0
        tx_packets = value("ckc_kafka_client_network_packets_total", role, "transmit") or 0.0
        count, denominator_source = denominator(role)
        result[role] = {result_key: count, "denominator_source": denominator_source, "rx_bytes": rx, "tx_bytes": tx, "total_bytes": rx + tx, "rx_packets": rx_packets, "tx_packets": tx_packets, "total_packets": rx_packets + tx_packets,
                        "rx_bytes_per_message": rx / count if count else None, "tx_bytes_per_message": tx / count if count else None, "bytes_per_message": (rx + tx) / count if count else None}
        total_rx += rx; total_tx += tx; total_rx_packets += rx_packets; total_tx_packets += tx_packets
    producer = result["producer"]
    consumer = result["consumer"]
    result["total"] = {"published_messages": producer["published_messages"], "consumer_records": consumer["consumer_records"], "consumer_denominator_source": consumer["denominator_source"], "rx_bytes": total_rx, "tx_bytes": total_tx, "total_bytes": total_rx + total_tx, "rx_packets": total_rx_packets, "tx_packets": total_tx_packets, "total_packets": total_rx_packets + total_tx_packets,
                       "rx_bytes_per_message": sum(value or 0 for value in (producer["rx_bytes_per_message"], consumer["rx_bytes_per_message"])), "tx_bytes_per_message": sum(value or 0 for value in (producer["tx_bytes_per_message"], consumer["tx_bytes_per_message"])), "bytes_per_message": sum(value or 0 for value in (producer["bytes_per_message"], consumer["bytes_per_message"]))}
    return result


def collect_report_time_series(
    client: PrometheusClient,
    start: datetime,
    duration_seconds: float,
) -> dict[str, list[dict[str, float]]]:
    end = start + timedelta(seconds=max(1, duration_seconds))
    step = max(5, min(60, int(max(1, duration_seconds) / 240)))
    return {
        name: [
            {"at_seconds": max(0.0, timestamp - start.timestamp()), "value": value}
            for timestamp, value in client.query_range(query, start, end, step)
        ]
        for name, query in REPORT_TIME_SERIES.items()
    }
