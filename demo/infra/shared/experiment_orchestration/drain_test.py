from __future__ import annotations

import unittest

from .drain import DRAINED, IDLE, ConsumerDrainTracker


class ConsumerDrainTrackerTest(unittest.TestCase):
    def test_reports_drained_after_zero_lag_is_stable(self) -> None:
        tracker = ConsumerDrainTracker(stable_seconds=15, idle_seconds=60)

        self.assertIsNone(tracker.observe(0, 100, 10))
        self.assertIsNone(tracker.observe(0, 100, 24))
        self.assertEqual(DRAINED, tracker.observe(0, 100, 25))

    def test_reports_idle_when_pending_lag_and_processing_do_not_progress(self) -> None:
        tracker = ConsumerDrainTracker(stable_seconds=15, idle_seconds=60)

        self.assertIsNone(tracker.observe(42, 100, 10))
        self.assertIsNone(tracker.observe(42, 100, 69))
        self.assertEqual(IDLE, tracker.observe(42, 100, 70))

    def test_processing_or_lag_progress_restarts_idle_window(self) -> None:
        tracker = ConsumerDrainTracker(stable_seconds=15, idle_seconds=60)

        self.assertIsNone(tracker.observe(42, 100, 10))
        self.assertIsNone(tracker.observe(42, 101, 50))
        self.assertIsNone(tracker.observe(41, 101, 100))
        self.assertIsNone(tracker.observe(41, 101, 159))
        self.assertEqual(IDLE, tracker.observe(41, 101, 160))

    def test_missing_lag_never_claims_that_pending_work_is_idle(self) -> None:
        tracker = ConsumerDrainTracker(stable_seconds=15, idle_seconds=60)

        self.assertIsNone(tracker.observe(None, 100, 10))
        self.assertIsNone(tracker.observe(None, 100, 1000))


if __name__ == "__main__":
    unittest.main()
