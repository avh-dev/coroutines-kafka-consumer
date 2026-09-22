from __future__ import annotations

from dataclasses import dataclass


DRAINED = "DRAINED"
IDLE = "IDLE"


@dataclass
class ConsumerDrainTracker:
    stable_seconds: float
    idle_seconds: float
    zero_since: float | None = None
    idle_since: float | None = None
    previous_lag: float | None = None
    previous_processed: float | None = None

    def observe(self, lag: float | None, processed: float | None, now: float) -> str | None:
        progressed = (
            self.previous_lag is not None
            and lag is not None
            and lag < self.previous_lag
        ) or (
            self.previous_processed is not None
            and processed is not None
            and processed > self.previous_processed
        )

        outcome = None
        if lag is not None and lag <= 0:
            self.idle_since = None
            if self.zero_since is None:
                self.zero_since = now
            if now - self.zero_since >= self.stable_seconds:
                outcome = DRAINED
        else:
            self.zero_since = None
            if lag is not None:
                if self.idle_since is None or progressed:
                    self.idle_since = now
                if now - self.idle_since >= self.idle_seconds:
                    outcome = IDLE
            else:
                self.idle_since = None

        self.previous_lag = lag
        self.previous_processed = processed
        return outcome

    def stable_for(self, now: float) -> float:
        return max(0.0, now - self.zero_since) if self.zero_since is not None else 0.0

    def idle_for(self, now: float) -> float:
        return max(0.0, now - self.idle_since) if self.idle_since is not None else 0.0
