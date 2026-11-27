"""Evidence for an audited two-pod shared-partition warm-up qualification."""

from __future__ import annotations

import re
from dataclasses import dataclass

EVIDENCE_FILE = "warmup-scale-up.json"
FAILED_EXIT_CODE = 2
MAPPINGS = {
    "order-events": ("order.events.v1", "ckc.traffic-warmup.v6.order.events", 1, 101),
    "batch-events": ("batch.events.v1", "ckc.traffic-warmup.v6.batch.events", 2, 102),
    "cauldron-events": ("cauldron.events.v1", "ckc.traffic-warmup.v6.cauldron.events", 3, 103),
}
LIFECYCLE = re.compile(
    r"Warm-up lifecycle: phase=(\w+), participantId=([^,\s]+), sessionId=([^,\s]+), reasonCode=([^,\s]+)"
)
FORWARDED = re.compile(
    r"Warm-up record forwarded: sessionId=([^,\s]+), "
    r"source=(\S+)-(\d+)@(\d+), destination=(\S+)-(\d+)@(\d+)"
)


@dataclass(frozen=True)
class Lifecycle:
    phase: str
    participant: str
    session: str
    reason: str


def lifecycle(logs: str) -> list[Lifecycle]:
    return [Lifecycle(*match.groups()) for match in LIFECYCLE.finditer(logs)]


def main_ready(events: list[Lifecycle]) -> bool:
    return MAPPINGS.keys() <= {event.participant for event in events if event.phase == "MAIN_READY"}


def donor_is_ready(logs: str) -> bool:
    events = lifecycle(logs)
    for index, event in enumerate(events):
        if event.phase == "COLD_FALLBACK" and event.reason == "incomplete_donor_coverage":
            return main_ready(events[index + 1 :])
    return False


def recipient_session(logs: str) -> str | None:
    events = lifecycle(logs)
    for index, event in enumerate(events):
        if event.phase == "COLD_FALLBACK" or (event.phase == "RECIPIENT_FINISHED" and event.reason != "null"):
            raise ValueError(f"Recipient warm-up failed: {event.phase} ({event.reason})")
        if event.phase != "RECIPIENT_FINISHED":
            continue
        if event.session == "null" or not any(
            start.phase == "WARMING" and start.session == event.session for start in events[:index]
        ):
            raise ValueError("Recipient completion has no matching WARMING event")
        if main_ready(events[index + 1 :]):
            return event.session
    return None


def forwarding_offsets(logs: str, session: str) -> dict[str, list[dict[str, list[int]]]]:
    offsets: dict[str, list[dict[str, list[int]]]] = {participant: [] for participant in MAPPINGS}
    participants = {(source, destination): participant for participant, (source, destination, _, _) in MAPPINGS.items()}
    for match in FORWARDED.finditer(logs):
        session_id, source, source_partition, source_offset, destination, dest_partition, dest_offset = match.groups()
        if session_id != session:
            continue
        participant = participants.get((source, destination))
        if participant is None or dest_partition != "0":
            raise ValueError(f"Unexpected forwarding coordinates in session {session}: {match[0]}")
        offsets[participant].append({
            "source": [int(source_partition), int(source_offset)],
            "destination": [int(dest_partition), int(dest_offset)],
        })
    return offsets


def qualification_requested(definition: dict) -> bool:
    return any(
        step.get("type") == "deployment_scale" and (step.get("params") or {}).get("verify_warmup") is True
        for step in definition.get("chaos_steps") or []
    )
