# Experiments

This module contains historical implementations and benchmarks
used to evaluate and select the final `OffsetTracker`.

There is intentionally **no production code** in this module.
All implementations live in test fixtures and are used only by
tests and JMH benchmarks.

The module is excluded from the build by default.

To execute test or run benchmarks use parameter `-PwithExperiments=true`