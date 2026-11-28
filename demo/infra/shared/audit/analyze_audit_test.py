#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import gzip
import io
import json
import contextlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


def load_analyzer():
    path = Path(__file__).with_name("analyze-audit.py")
    spec = importlib.util.spec_from_file_location("analyze_audit", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load analyzer module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyzer = load_analyzer()


class WarmupScaleUpAuditTest(unittest.TestCase):
    def test_reconciles_all_mappings_and_fails_with_missing_or_ambiguous_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "run-metadata.json"
            metadata.write_text(json.dumps({"run_id": "test"}), encoding="utf-8")
            proof = root / "warmup-scale-up.json"
            audit = root / "audit.log.gz"
            command = [
                sys.executable, analyzer.__file__, "--input-file", str(audit), "--require-records",
                "--metadata-file", str(metadata), "--warmup-scale-up-evidence", str(proof),
            ]
            participants = ["order-events", "batch-events", "cauldron-events"]
            evidence = {
                "status": "passed", "runId": "test", "scaleAt": "1970-01-01T00:00:02Z",
                "session": "session", "donor": {"pod": "pod-1"}, "recipient": {"pod": "pod-2"},
                "offsets": {
                    participant: [{"source": [2, 7], "destination": [0, 5]}]
                    for participant in participants
                },
            }
            records = [
                line
                for topic in (1, 2, 3)
                for line in (
                    f"P|{topic}|2|1|1000|1000|cold",
                    f"C|{topic}|2|1|1005|cold",
                    f"P|{topic}|2|7|3000|3000|warm",
                    f"C|{topic + 100}|0|5|3005|warm",
                    f"C|{topic}|2|7|3006|warm",
                )
            ]
            for label, lines, qualified, error in (
                ("successful", records, evidence, None),
                ("missing donor audit", [line for line in records if line != "C|3|2|7|3006|warm"],
                 evidence, "Source delivery failed"),
                ("incorrect recipient key", [line.replace("C|103|0|5|3005|warm", "C|103|0|5|3005|wrong") for line in records],
                 evidence, "Forwarding audit mismatch"),
                ("unmatched recipient offset", [*records, "C|101|0|6|3005|warm"], evidence, "no matching forwarding event"),
                ("duplicate recipient audit", [*records, "C|101|0|5|3005|warm"], evidence, "Forwarding audit mismatch"),
                ("cold recipient", records, evidence | {"status": "failed", "error": "Recipient COLD_FALLBACK"}, "COLD_FALLBACK"),
                ("missing proof", records, None, "No such file"),
            ):
                with self.subTest(label=label):
                    if qualified is None:
                        proof.unlink()
                    else:
                        proof.write_text(json.dumps(qualified), encoding="utf-8")
                    with gzip.open(audit, "wt", encoding="utf-8") as output:
                        output.write("\n".join(lines) + "\n")
                    completed = subprocess.run(command, capture_output=True, text=True)
                    document = yaml.safe_load(completed.stdout)["audit"]
                    result = document["warmup_scale_up"]
                    if error is None:
                        self.assertEqual(0, completed.returncode, completed.stderr)
                        self.assertEqual("passed", result["status"])
                        self.assertEqual(dict.fromkeys(participants, 1), result["processed_by_participant"])
                        self.assertEqual(6, document["totals"]["processed"])
                        self.assertEqual(0, document["totals"]["without_publish"]["processed"])
                        self.assertEqual(3, len(document["topics"]))
                    else:
                        self.assertEqual(2, completed.returncode, completed.stderr)
                        self.assertEqual("failed", result["status"])
                        self.assertIn(error, result["error"])


def latency_rule(
    max_ms: int = 1000,
    allowed_exceed_percent: float = 1.0,
) -> object:
    return analyzer.LatencySlaRule(
        id="business-events",
        title="Business events",
        topic_ids=frozenset({1, 2}),
        topics=("order.events.v1", "batch.events.v1"),
        max_ms=max_ms,
        allowed_exceed_percent=allowed_exceed_percent,
    )


def analyze(
    lines: list[str],
    open_record_ttl_ms: int | None = None,
    latency_sla_rules: tuple[object, ...] = (),
    scenario_start_ms: int | None = None,
    latency_limits_ms: dict[int, int | float] | None = None,
) -> dict[str, object]:
    accumulator = analyzer.AuditAccumulator(
        open_record_ttl_ms=open_record_ttl_ms,
        scenario_start_ms=scenario_start_ms,
        latency_sla_rules=latency_sla_rules,
        latency_limits_ms=latency_limits_ms,
    )
    for line in lines:
        accumulator.add(analyzer.parse_record(line))
    accumulator.finish()
    return analyzer.summary_document(accumulator, {})


class AuditAnalyzerFairnessTest(unittest.TestCase):
    def test_timeline_uses_kafka_publication_time_and_completion_time(self) -> None:
        start_ms = 1_000_000
        document = analyze(
            [
                f"P|1|0|1|{start_ms + 58_000}|{start_ms + 59_000}|late",
                f"C|1|0|1|{start_ms + 72_000}|late",
            ],
            scenario_start_ms=start_ms,
            latency_limits_ms={1: 2_000},
        )

        timeline = document["audit"]["topics"]["order.events.v1"]["timeline"]
        points = {point["second"]: point for point in timeline["points"]}
        self.assertEqual(1, points[58]["published"])
        self.assertEqual(0, points[58]["processed"])
        self.assertEqual(1, points[72]["processed"])
        self.assertEqual(14_000, document["audit"]["topics"]["order.events.v1"]["e2e_latency"]["max"])

    def test_exact_slow_count_can_exist_without_a_p99_episode(self) -> None:
        start_ms = 1_000_000
        document = analyze(
            [
                f"P|1|0|1|{start_ms + 1_000}|{start_ms + 1_000}|slow",
                f"C|1|0|1|{start_ms + 4_000}|slow",
            ],
            scenario_start_ms=start_ms,
            latency_limits_ms={1: 2_000},
        )

        topic = document["audit"]["topics"]["order.events.v1"]
        self.assertEqual(1, topic["e2e_latency"]["exceeded"])
        self.assertEqual([], topic["timeline"]["episodes"])
        self.assertEqual("UNAVAILABLE", topic["timeline"]["status"])
        self.assertEqual("UNAVAILABLE", topic["timeline"]["episode_status"])

    def test_timeline_episode_is_strictly_above_limit_and_keeps_peak_sample_count(self) -> None:
        start_ms = 1_000_000
        lines = []
        for offset in range(100):
            published = start_ms + 10_000
            lines.extend((
                f"P|1|0|{offset + 1}|{published}|{published}|slow-{offset}",
                f"C|1|0|{offset + 1}|{start_ms + 13_000}|slow-{offset}",
            ))
        document = analyze(
            lines,
            scenario_start_ms=start_ms,
            latency_limits_ms={1: 2_000},
        )

        timeline = document["audit"]["topics"]["order.events.v1"]["timeline"]
        self.assertEqual(1, len(timeline["episodes"]))
        episode = timeline["episodes"][0]
        self.assertEqual(13, episode["start_second"])
        self.assertEqual(3_000, episode["peak_p99_ms"])
        self.assertEqual(100, episode["peak_p99_samples"])

    def test_lifecycle_handoff_success_is_present_once_in_processed_timeline(self) -> None:
        start_ms = 1_000_000
        for ttl in (None, 60_000):
            with self.subTest(open_record_ttl_ms=ttl):
                document = analyze(
                    [
                        f"P|1|0|1|{start_ms + 1_000}|{start_ms + 1_000}|handoff",
                        f"D|1|0|1|{start_ms + 1_100}|handoff|lifecycle",
                        f"C|1|0|1|{start_ms + 2_000}|handoff",
                        f"C|1|0|1|{start_ms + 2_100}|handoff",
                    ],
                    open_record_ttl_ms=ttl,
                    scenario_start_ms=start_ms,
                )
                topic = document["audit"]["topics"]["order.events.v1"]
                self.assertEqual(1, topic["processed"])
                self.assertEqual(1, sum(point["processed"] for point in topic["timeline"]["points"]))
                self.assertEqual(1, topic["timeline"]["points"][2]["processed"])
                self.assertEqual(1, topic["duplicates"]["processed"])

    def test_full_observation_boundary_keeps_quiet_topic_tail_for_rolling_p99(self) -> None:
        start_ms = 1_000_000
        lines = []
        for offset in range(100):
            lines.extend((
                f"P|1|0|{offset + 1}|{start_ms + 10_000}|{start_ms + 10_000}|slow-{offset}",
                f"C|1|0|{offset + 1}|{start_ms + 13_000}|slow-{offset}",
            ))
        lines.extend((
            f"P|2|0|1|{start_ms + 40_000}|{start_ms + 40_000}|boundary",
            f"C|2|0|1|{start_ms + 40_100}|boundary",
        ))
        document = analyze(
            lines,
            scenario_start_ms=start_ms,
            latency_limits_ms={1: 2_000},
        )

        timeline = document["audit"]["topics"]["order.events.v1"]["timeline"]
        self.assertEqual(41, timeline["observed_until_second"])
        self.assertEqual(5, timeline["episodes"][0]["duration_seconds"])
        self.assertEqual(41, len(timeline["points"]))

    def test_yaml_serialization_preserves_empty_timeline_lists(self) -> None:
        start_ms = 1_000_000
        accumulator = analyzer.AuditAccumulator(open_record_ttl_ms=None, scenario_start_ms=start_ms)
        accumulator.add(analyzer.parse_record(
            f"P|1|0|1|{start_ms + 1_000}|{start_ms + 1_000}|published"
        ))
        accumulator.finish()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            analyzer.print_yaml(analyzer.summary_document(accumulator, {}))
        document = yaml.safe_load(output.getvalue())
        timeline = document["audit"]["topics"]["order.events.v1"]["timeline"]
        self.assertIn("timeline", document["audit"])
        self.assertNotIn("timeline", document["audit"]["totals"])
        self.assertIsInstance(timeline["points"], list)
        self.assertIsInstance(timeline["episodes"], list)
        self.assertEqual([], timeline["episodes"])

    def test_measurement_window_clips_full_completion_episodes(self) -> None:
        start_ms = 1_000_000
        window = analyzer.MeasurementWindow("load", start_ms, start_ms + 60_000)
        accumulator = analyzer.MultiWindowAuditAccumulator(
            open_record_ttl_ms=None,
            windows=(window,),
            scenario_start_ms=start_ms,
            latency_limits_ms={1: 2_000},
        )
        for offset in range(100):
            accumulator.add(analyzer.parse_record(
                f"P|1|0|{offset + 1}|{start_ms + 58_000}|{start_ms + 58_000}|late-{offset}"
            ))
            accumulator.add(analyzer.parse_record(
                f"C|1|0|{offset + 1}|{start_ms + 72_000}|late-{offset}"
            ))
        accumulator.add(analyzer.parse_record(
            f"P|2|0|1|{start_ms + 90_000}|{start_ms + 90_000}|boundary"
        ))
        accumulator.add(analyzer.parse_record(
            f"C|2|0|1|{start_ms + 90_100}|boundary"
        ))
        accumulator.finish()

        audit = analyzer.summary_document(accumulator, {})["audit"]
        self.assertEqual(1, len(audit["topics"]["order.events.v1"]["timeline"]["episodes"]))
        window_audit = audit["measurement_windows"][0]
        self.assertEqual([], window_audit["timeline"]["episodes"])
        self.assertEqual("UNAVAILABLE", window_audit["timeline"]["status"])
        self.assertEqual("UNAVAILABLE", window_audit["timeline"]["episode_status"])
        self.assertEqual("full_target_completion_timeline", window_audit["timeline"]["source"])
        self.assertEqual([], window_audit["topics"]["order.events.v1"]["episode_intersections"])

    def test_window_totals_merge_topic_latency_including_late_completion(self) -> None:
        accumulator = analyzer.MultiWindowAuditAccumulator(
            open_record_ttl_ms=None,
            windows=(analyzer.MeasurementWindow("load", 1000, 2000),),
            scenario_start_ms=1000,
        )
        for line in (
            "P|1|0|1|1100|1100|order",
            "P|2|0|1|1200|1200|batch",
            "C|1|0|1|1600|order",
            "P|1|0|2|2000|2000|outside-window",
            "C|2|0|1|2700|batch",
            "C|1|0|2|5000|outside-window",
        ):
            accumulator.add(analyzer.parse_record(line))
        accumulator.finish()

        window, = analyzer.summary_document(accumulator, {})["audit"]["measurement_windows"]
        latency = window["totals"]["e2e_latency"]
        self.assertEqual(2, latency["count"])
        self.assertEqual(1490, latency["p99"])
        self.assertEqual(1500, latency["max"])

    def test_fractional_window_projects_points_peaks_and_observation_consistently(self) -> None:
        start_ms = 1_000_000
        stats = analyzer.TimelineStats(start_ms)
        for second, latency_ms, count in ((4, 5000, 1000), (9, 3000, 100)):
            for _ in range(count):
                stats.add_latency(start_ms + second * 1000, latency_ms)
        timeline = stats.summary(2000, observed_until_second=10)
        self.assertEqual(5000, timeline["episodes"][0]["peak_p999_ms"])

        def project(start_offset_ms: int, end_offset_ms: int) -> dict[str, object]:
            window = analyzer.MeasurementWindow(
                "window", start_ms + start_offset_ms, start_ms + end_offset_ms,
            )
            return analyzer.clipped_timeline(timeline, window, start_ms)

        fractional = project(9500, 9750)
        self.assertEqual([9], [point["second"] for point in fractional["points"]])
        self.assertEqual((9.5, 9.75), (
            fractional["observed_from_second"], fractional["observed_until_second"],
        ))
        self.assertEqual("HAS_EPISODES", fractional["episode_status"])
        self.assertEqual(0.25, fractional["computed_p99_seconds"])
        self.assertEqual(0, fractional["unavailable_p99_seconds"])
        episode, = fractional["episodes"]
        self.assertEqual((9.5, 9.75, 0.25), (
            episode["start_second"], episode["end_second"], episode["duration_seconds"],
        ))
        self.assertEqual(3000, episode["peak_p99_ms"])
        self.assertEqual(100, episode["peak_p99_samples"])
        self.assertIsNone(episode["peak_p999_ms"])
        self.assertIsNone(episode["peak_p999_samples"])

        mixed = project(3750, 4500)
        self.assertEqual(0.5, mixed["computed_p99_seconds"])
        self.assertEqual(0.25, mixed["unavailable_p99_seconds"])

        truncated = project(9000, 15000)
        self.assertEqual(15, truncated["window_end_second"])
        self.assertEqual((9, 11), (
            truncated["observed_from_second"], truncated["observed_until_second"],
        ))
        self.assertEqual([9, 10], [point["second"] for point in truncated["points"]])
        self.assertEqual(2, truncated["computed_p99_seconds"])
        self.assertEqual(0, truncated["unavailable_p99_seconds"])
        outside = project(11000, 15000)
        self.assertIsNone(outside["observed_from_second"])
        self.assertIsNone(outside["observed_until_second"])
        self.assertEqual([], outside["points"])
        self.assertEqual([], outside["episodes"])
        self.assertEqual("UNAVAILABLE", outside["episode_status"])

    def test_target_flow_excludes_diagnostic_topics_but_keeps_them_in_topic_evidence(self) -> None:
        start_ms = 1_000_000
        lines = [
            f"P|{topic}|0|1|{start_ms + 1_000}|{start_ms + 1_000}|app-{topic}"
            for topic in (1, 2, 3)
        ]
        lines.extend(
            f"C|{topic}|0|1|{start_ms + 1_100}|app-{topic}"
            for topic in (1, 2, 3)
        )
        lines.append(f"C|101|0|1|{start_ms + 1_200}|warmup")
        document = analyze(lines, scenario_start_ms=start_ms)
        audit = document["audit"]

        self.assertEqual(3, audit["totals"]["published"])
        self.assertEqual(3, audit["totals"]["processed"])
        self.assertEqual(
            1,
            audit["topics"]["unknown-topic-101"]["without_publish"]["processed"],
        )
        self.assertIn("unknown-topic-101", audit["timeline"]["diagnostic_topics"])
        self.assertEqual(3, sum(point["published"] for point in audit["timeline"]["points"]))
        self.assertEqual(3, sum(point["processed"] for point in audit["timeline"]["points"]))

    def test_single_pass_accumulates_complete_and_named_publication_windows(self) -> None:
        accumulator = analyzer.MultiWindowAuditAccumulator(
            open_record_ttl_ms=None,
            windows=(
                analyzer.MeasurementWindow("baseline", 1000, 2000),
                analyzer.MeasurementWindow("degraded", 3000, 4000),
            ),
        )
        for line in (
            "C|1|0|1|1500|order-a",
            "P|1|0|1|1100|1200|order-a",
            "P|1|0|2|2100|2200|order-between",
            "C|1|0|2|2300|order-between",
            "P|1|0|3|3100|3200|order-b",
            "C|1|0|3|3500|order-b",
        ):
            accumulator.add(analyzer.parse_record(line))
        accumulator.finish()

        audit = analyzer.summary_document(accumulator, {})["audit"]
        self.assertEqual(3, audit["totals"]["published"])
        self.assertEqual(3, audit["totals"]["processed"])
        windows = {window["name"]: window for window in audit["measurement_windows"]}
        self.assertEqual(1, windows["baseline"]["totals"]["published"])
        self.assertEqual(1, windows["baseline"]["totals"]["processed"])
        self.assertEqual(400, windows["baseline"]["totals"]["e2e_latency"]["max"])
        self.assertEqual(1, windows["degraded"]["totals"]["published"])
        self.assertEqual(1, windows["degraded"]["totals"]["processed"])

    def test_preselected_publication_cohort_keeps_terminal_seen_before_publish(self) -> None:
        key = analyzer.RecordKey(1, 0, 7)
        accumulator = analyzer.AuditAccumulator(open_record_ttl_ms=None, cohort_keys={key})
        accumulator.add(analyzer.parse_record("C|1|0|7|1500|order-a"))
        accumulator.add(analyzer.parse_record("P|1|0|7|1000|1100|order-a"))
        accumulator.finish()
        totals = analyzer.summary_document(accumulator, {})["audit"]["totals"]
        self.assertEqual(1, totals["published"])
        self.assertEqual(1, totals["processed"])
        self.assertEqual(0, totals["missing_terminal"])
        self.assertEqual(500, totals["e2e_latency"]["max"])

    def test_reports_topic_e2e_percentiles_and_limit_exceedances(self) -> None:
        accumulator = analyzer.AuditAccumulator(open_record_ttl_ms=None, latency_limits_ms={1: 1000})
        for line in (
            "P|1|0|1|1000|1000|order-a",
            "C|1|0|1|1500|order-a",
            "P|1|0|2|2000|2000|order-b",
            "C|1|0|2|3501|order-b",
        ):
            accumulator.add(analyzer.parse_record(line))
        accumulator.finish()
        e2e = analyzer.summary_document(accumulator, {})["audit"]["topics"]["order.events.v1"]["e2e_latency"]

        self.assertEqual(1000, e2e["limit_ms"])
        self.assertEqual(2, e2e["count"])
        self.assertEqual(1, e2e["exceeded"])
        self.assertEqual(50.0, e2e["exceeded_percent"])
        self.assertEqual(1000.5, e2e["p50"])

    def test_reports_exact_processed_latency_sla_violations(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "C|1|0|1|2000|order-a",
                "P|1|0|2|3000|3000|order-b",
                "C|1|0|2|4001|order-b",
                "P|2|0|1|5000|5000|batch-a",
                "C|2|0|1|305000|batch-a",
                "P|3|0|1|6000|6000|telemetry-a",
                "C|3|0|1|606000|telemetry-a",
            ],
            latency_sla_rules=(latency_rule(allowed_exceed_percent=50),),
        )

        totals = document["audit"]["totals"]["latency_sla"]["rules"][0]
        order = document["audit"]["topics"]["order.events.v1"]["latency_sla"]["rules"][0]
        batch = document["audit"]["topics"]["batch.events.v1"]["latency_sla"]["rules"][0]

        self.assertEqual(3, totals["processed"])
        self.assertEqual(3, totals["measured"])
        self.assertEqual(2, totals["exceeded"])
        self.assertEqual(66.666667, totals["exceeded_percent"])
        self.assertEqual(300000, totals["max_observed_ms"])
        self.assertEqual("FAIL", totals["status"])
        self.assertEqual(1, order["exceeded"])
        self.assertEqual(1, batch["exceeded"])
        self.assertNotIn("latency_sla", document["audit"]["topics"]["cauldron.events.v1"])

    def test_latency_equal_to_limit_passes_and_allowed_percentage_is_inclusive(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "C|1|0|1|2000|order-a",
                "P|1|0|2|3000|3000|order-b",
                "C|1|0|2|4001|order-b",
            ],
            latency_sla_rules=(latency_rule(allowed_exceed_percent=50),),
        )

        latency = document["audit"]["totals"]["latency_sla"]["rules"][0]
        self.assertEqual(1, latency["within_sla"])
        self.assertEqual(1, latency["exceeded"])
        self.assertEqual(50.0, latency["exceeded_percent"])
        self.assertEqual("PASS", latency["status"])

    def test_unmatched_or_negative_timestamp_processing_is_incomplete(self) -> None:
        document = analyze(
            [
                "C|1|0|1|2000|order-a",
                "P|1|0|2|5000|5000|order-b",
                "C|1|0|2|4000|order-b",
            ],
            latency_sla_rules=(latency_rule(),),
        )

        latency = document["audit"]["totals"]["latency_sla"]["rules"][0]
        self.assertEqual(2, latency["processed"])
        self.assertEqual(0, latency["measured"])
        self.assertEqual(2, latency["unmeasured"])
        self.assertEqual(1, latency["invalid_negative_latency"])
        self.assertEqual("INCOMPLETE", latency["status"])

    def test_exact_matching_keeps_long_delayed_terminal_records(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "C|1|0|1|72000|order-a",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual("exact", document["audit"]["delivery_matching"]["mode"])
        self.assertIsNone(document["audit"]["delivery_matching"]["open_record_ttl_seconds"])
        self.assertEqual(0, totals["missing_terminal"])
        self.assertEqual(0, totals["without_publish"]["processed"])

    def test_exact_matching_compacts_closed_offsets_and_keeps_late_outcomes_exact(self) -> None:
        accumulator = analyzer.AuditAccumulator(open_record_ttl_ms=None)
        second_page_offset = analyzer.OFFSET_PAGE_SIZE + 2
        for line in (
            "P|1|0|1|1000|1000|order-a",
            "C|1|0|1|1100|order-a",
            f"P|1|0|{second_page_offset}|2000|2000|order-b",
            f"C|1|0|{second_page_offset}|2100|order-b",
            "P|1|0|1|1000|2200|order-a",
            "C|1|0|1|2300|order-a",
            "F|1|0|1|2400|order-a",
        ):
            accumulator.add(analyzer.parse_record(line))

        stats = accumulator.by_topic[1]
        self.assertEqual(0, len(stats.open_by_key))
        self.assertEqual(0, len(stats.recent_closed_by_key))
        self.assertEqual(2, len(stats.closed_offsets.pages))
        accumulator.finish()

        totals = analyzer.summary_document(accumulator, {})["audit"]["totals"]
        self.assertEqual(2, totals["published"])
        self.assertEqual(2, totals["processed"])
        self.assertEqual(1, totals["duplicates"]["published"])
        self.assertEqual(1, totals["duplicates"]["processed"])
        self.assertEqual(1, totals["failed"])
        self.assertEqual(1, totals["conflicting_terminal_outcomes"])

    def test_exact_key_order_is_reduced_from_shared_spool_for_complete_and_window(self) -> None:
        accumulator = analyzer.MultiWindowAuditAccumulator(
            open_record_ttl_ms=None,
            windows=(analyzer.MeasurementWindow("steady", 1000, 5000),),
        )
        for offset, timestamp in ((10, 1000), (12, 2000), (11, 3000)):
            accumulator.add(
                analyzer.parse_record(
                    f"P|1|0|{offset}|{timestamp}|{timestamp}|same-order"
                )
            )
            accumulator.add(
                analyzer.parse_record(
                    f"C|1|0|{offset}|{timestamp + 100}|same-order"
                )
            )
        accumulator.finish()

        audit = analyzer.summary_document(accumulator, {})["audit"]
        topic = audit["topics"]["order.events.v1"]
        window_topic = audit["measurement_windows"][0]["topics"]["order.events.v1"]
        self.assertEqual(1, topic["ordering"]["by_key"]["out_of_order"])
        self.assertEqual(1, window_topic["ordering"]["by_key"]["out_of_order"])
        self.assertFalse(Path(accumulator.key_order_spool.directory.name).exists())

    def test_distinguishes_expected_replay_drops_from_unexpected_terminal_order(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1000|cauldron-a",
                "C|3|0|1|1100|cauldron-a",
                "D|3|0|1|1200|cauldron-a|stale_age",
                "P|3|0|2|2000|2000|cauldron-b",
                "D|3|0|2|2100|cauldron-b|stale_age",
                "C|3|0|2|2200|cauldron-b",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(2, totals["conflicting_terminal_outcomes"])
        self.assertEqual(1, totals["replay_dropped_after_processed"])
        self.assertEqual(
            {"stale_age": 1},
            totals["replay_dropped_after_processed_by_reason"],
        )
        self.assertEqual(1, totals["unexpected_terminal_outcomes"])

    def test_when_lifecycle_drop_is_reprocessed_then_exact_matching_reports_handoff(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "D|1|0|1|1100|order-a|lifecycle",
                "C|1|0|1|1200|order-a",
            ]
        )

        self.assert_lifecycle_handoff(document)

    def test_when_lifecycle_drop_is_reprocessed_then_bounded_matching_reports_handoff(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "D|1|0|1|1100|order-a|lifecycle",
                "C|1|0|1|1200|order-a",
            ],
            open_record_ttl_ms=60_000,
        )

        self.assert_lifecycle_handoff(document)

    def test_when_lifecycle_drop_precedes_publish_then_open_record_reports_handoff(self) -> None:
        document = analyze(
            [
                "D|1|0|1|1000|order-a|lifecycle",
                "C|1|0|1|1100|order-a",
                "P|1|0|1|1200|1200|order-a",
            ]
        )

        self.assert_lifecycle_handoff(document)

    def test_when_explicit_handoff_is_reprocessed_then_it_is_nonterminal_evidence(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "H|1|0|1|1100|order-a|assignment_ended",
                "C|1|0|1|1200|order-a",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(1, totals["processed"])
        self.assertEqual(0, totals["dropped"])
        self.assertEqual(1, totals["handoff_records"])
        self.assertEqual(1, totals["handoff_unique"])
        self.assertEqual({"assignment_ended": 1}, totals["handoff_by_reason"])
        self.assertEqual(1, totals["lifecycle_handoff_reprocessed"])
        self.assertEqual(0, totals["conflicting_terminal_outcomes"])
        self.assertEqual(0, totals["missing_terminal"])

    def test_when_explicit_handoff_has_no_later_terminal_then_record_remains_missing(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "H|1|0|1|1100|order-a|assignment_ended",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(0, totals["dropped"])
        self.assertEqual(1, totals["handoff_records"])
        self.assertEqual(1, totals["missing_terminal"])

    def test_when_lifecycle_discard_is_terminal_then_it_is_not_treated_as_legacy_handoff(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1000|cauldron-a",
                "D|3|0|1|1100|cauldron-a|lifecycle_discard",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(1, totals["dropped"])
        self.assertEqual({"lifecycle_discard": 1}, totals["dropped_by_reason"])
        self.assertEqual(0, totals["handoff_records"])
        self.assertEqual(0, totals["missing_terminal"])

    def test_when_handoff_refreshes_bounded_ttl_then_unresolved_record_is_evicted(self) -> None:
        accumulator = analyzer.AuditAccumulator(open_record_ttl_ms=100)
        published = analyzer.parse_record("P|1|0|1|1000|1000|order-a")
        accumulator.add(published)
        accumulator.add(analyzer.parse_record("H|1|0|1|1100|order-a|assignment_ended"))
        accumulator.add(analyzer.parse_record("P|1|0|2|2500|2500|order-b"))

        topic_stats = accumulator.by_topic[1]
        self.assertNotIn(published.key, topic_stats.open_by_key)
        self.assertEqual(1, topic_stats.missing_terminal)
        accumulator.finish()

    def assert_lifecycle_handoff(self, document: dict[str, object]) -> None:
        totals = document["audit"]["totals"]
        self.assertEqual(1, totals["processed"])
        self.assertEqual(0, totals["dropped"])
        self.assertEqual({}, totals["dropped_by_reason"])
        self.assertEqual(1, totals["handoff_records"])
        self.assertEqual({"assignment_ended": 1}, totals["handoff_by_reason"])
        self.assertEqual(1, totals["lifecycle_handoff_reprocessed"])
        self.assertEqual(0, totals["conflicting_terminal_outcomes"])
        self.assertEqual(0, totals["unexpected_terminal_outcomes"])
        self.assertEqual(0, totals["missing_terminal"])

    def test_bounded_matching_can_evict_long_delayed_terminal_records(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "C|1|0|1|72000|order-a",
            ],
            open_record_ttl_ms=60_000,
        )

        totals = document["audit"]["totals"]
        self.assertEqual("bounded_ttl", document["audit"]["delivery_matching"]["mode"])
        self.assertEqual(60.0, document["audit"]["delivery_matching"]["open_record_ttl_seconds"])
        self.assertEqual(1, totals["missing_terminal"])
        self.assertEqual(1, totals["without_publish"]["processed"])

    def test_retry_attempts_do_not_count_as_terminal_conflicts(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "R|1|0|1|1100|order-a",
                "C|1|0|1|1200|order-a",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(1, totals["retry_attempts"])
        self.assertEqual(1, totals["processed"])
        self.assertEqual(1, totals["terminal"])
        self.assertEqual(0, totals["failed"])
        self.assertEqual(0, totals["conflicting_terminal_outcomes"])

    def test_counts_unique_drop_reasons(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1000|cauldron-a",
                "D|3|0|1|1100|cauldron-a|stale_age",
                "D|3|0|1|1200|cauldron-a|stale_age",
                "P|3|0|2|2000|2000|cauldron-b",
                "D|3|0|2|2100|cauldron-b",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(2, totals["dropped"])
        self.assertEqual({"stale_age": 1, "unknown": 1}, totals["dropped_by_reason"])
        self.assertEqual(1, totals["duplicates"]["dropped"])

    def test_reports_skewed_cauldron_processing_fairness(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1010|cauldron-a",
                "C|3|0|1|1200|cauldron-a",
                "P|3|0|2|2000|2010|cauldron-a",
                "D|3|0|2|2020|cauldron-a",
                "P|3|0|3|3000|3010|cauldron-b",
                "D|3|0|3|3020|cauldron-b",
                "P|3|0|4|4000|4010|cauldron-b",
                "D|3|0|4|4020|cauldron-b",
                "P|3|0|5|5000|5010|cauldron-c",
                "C|3|0|5|5200|cauldron-c",
                "P|3|0|6|6000|6010|cauldron-c",
                "C|3|0|6|8200|cauldron-c",
            ]
        )

        fairness = document["audit"]["topics"]["cauldron.events.v1"]["key_fairness"]

        self.assertEqual(3, fairness["keys"])
        self.assertEqual(1, fairness["keys_without_processed"])
        self.assertEqual(0.444444, fairness["processed_ratio"]["gini"])
        self.assertEqual(0.816497, fairness["processed_ratio"]["coefficient_of_variation"])
        self.assertEqual(0, fairness["processed_max_gap_ms"]["keys_over_5s"])
        self.assertEqual(3000, fairness["processed_max_gap_ms"]["max"])
        self.assertEqual({3000: 1}, fairness["processed_max_gap_ms"]["histogram_ms"])
        self.assertEqual(3, fairness["record_age"]["processed"]["count"])
        self.assertEqual(2200, fairness["record_age"]["processed"]["max_ms"])

    def test_reports_dropped_records_before_processed_distribution(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1000|cauldron-a",
                "C|3|0|1|1100|cauldron-a",
                "P|3|0|2|1200|1200|cauldron-a",
                "D|3|0|2|1210|cauldron-a|replaced_by_newer_key_record",
                "P|3|0|3|1300|1300|cauldron-a",
                "D|3|0|3|1310|cauldron-a|replaced_by_newer_key_record",
                "P|3|0|4|1500|1500|cauldron-a",
                "C|3|0|4|1500|cauldron-a",
                "P|3|0|5|2000|2000|cauldron-b",
                "D|3|0|5|2050|cauldron-b|stale_age",
                "P|3|0|6|2500|2500|cauldron-b",
                "C|3|0|6|2600|cauldron-b",
                "P|3|0|7|3000|3000|cauldron-c",
                "D|3|0|7|3050|cauldron-c|stale_age",
            ]
        )

        freshness_gap = document["audit"]["topics"]["cauldron.events.v1"]["key_fairness"]["freshness_gap"]

        self.assertEqual(3, freshness_gap["processed_records"])
        self.assertEqual({0: 1, 1: 2, 2: 1}, freshness_gap["dropped_before_processed_histogram"])
        self.assertEqual({1: 1}, freshness_gap["trailing_dropped_before_processed_histogram"])
        self.assertEqual(1, freshness_gap["dropped_before_processed"]["p50"])
        self.assertEqual(2, freshness_gap["dropped_before_processed"]["max"])
        self.assertEqual(2, freshness_gap["first_drop_to_processed_ms"]["count"])
        self.assertEqual(550, freshness_gap["first_drop_to_processed_ms"]["max_ms"])
        self.assertEqual(190, freshness_gap["last_drop_to_processed_ms"]["min_ms"])
        self.assertEqual(1, freshness_gap["keys_with_pending_drops"])
        self.assertEqual(1, freshness_gap["pending_dropped_records"])

    def test_reports_even_cauldron_processing_fairness(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1010|cauldron-a",
                "C|3|0|1|1100|cauldron-a",
                "P|3|0|2|2000|2010|cauldron-b",
                "C|3|0|2|2100|cauldron-b",
                "P|3|0|3|3000|3010|cauldron-c",
                "C|3|0|3|3100|cauldron-c",
            ]
        )

        fairness = document["audit"]["topics"]["cauldron.events.v1"]["key_fairness"]

        self.assertEqual(3, fairness["keys"])
        self.assertEqual(0, fairness["keys_without_processed"])
        self.assertEqual(0.0, fairness["processed_ratio"]["gini"])
        self.assertEqual(0.0, fairness["processed_ratio"]["coefficient_of_variation"])
        self.assertEqual(0, fairness["processed_max_gap_ms"]["keys_over_5s"])
        self.assertEqual(100, fairness["record_age"]["processed"]["max_ms"])


if __name__ == "__main__":
    unittest.main()
