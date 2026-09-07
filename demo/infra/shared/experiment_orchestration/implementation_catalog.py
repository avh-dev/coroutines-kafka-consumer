from __future__ import annotations

import copy
from typing import Any


TOPIC_DEFAULTS = {
    "order": {
        "parallelism": ["workers"],
        "mode_parallelism": {"AT_LEAST_ONCE_PARTITION_ORDERING": ["partitions", "workers"]},
        "allowed_processing_modes": [
            "AT_LEAST_ONCE_KEY_ORDERING",
            "AT_LEAST_ONCE_PARTITION_ORDERING",
            "AT_LEAST_ONCE_NO_ORDERING",
        ],
    },
    "batch": {
        "parallelism": ["workers"],
        "mode_parallelism": {"AT_LEAST_ONCE_PARTITION_ORDERING": ["partitions", "workers"]},
        "allowed_processing_modes": [
            "AT_LEAST_ONCE_KEY_ORDERING",
            "AT_LEAST_ONCE_PARTITION_ORDERING",
            "AT_LEAST_ONCE_NO_ORDERING",
        ],
    },
    "telemetry": {
        "parallelism": ["workers"],
        "mode_parallelism": {"AT_LEAST_ONCE_PARTITION_ORDERING": ["partitions", "workers"]},
        "allowed_processing_modes": [
            "FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY",
            "FRESHNESS_FIRST_DROP_OLDEST",
            "AT_LEAST_ONCE_KEY_ORDERING",
            "AT_LEAST_ONCE_PARTITION_ORDERING",
            "AT_LEAST_ONCE_NO_ORDERING",
            "HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED",
        ],
    },
}


def profile_catalog(topics: dict[str, Any]) -> dict[str, Any]:
    """Return planner-owned implementation capabilities for a resolved experiment.

    Experiment YAML selects an implementation and explicitly supplies its measured
    settings.  This catalog only maps that stable implementation identity to its
    Spring profile and validates runtime capabilities.
    """
    planner_topics = {
        topic: {
            "kafka_topic": str(settings["kafka_topic"]),
            "traffic_percent_key": {
                "order": "order_event_percent",
                "batch": "batch_event_percent",
                "telemetry": "cauldron_telemetry_percent",
            }[topic],
        }
        for topic, settings in topics.items()
    }
    profiles: dict[str, dict[str, Any]] = {}
    for name, spring_profile, dispatcher in (
        ("ckc", "ckc", ("FIXED", ["DEFAULT", "FIXED", "IO", "VIRTUAL"])),
        ("ckc-sync", "ckc-sync", ("IO", ["IO", "VIRTUAL"])),
        ("ckc-spring-boot", "ckc-spring-boot", ("FIXED", ["DEFAULT", "FIXED", "IO", "VIRTUAL"])),
        ("confluent-reactor", "confluent-parallel-reactor", ("FIXED", ["DEFAULT", "FIXED", "IO", "VIRTUAL"])),
        ("spring-kafka-coroutines-naive", "spring-kafka-coroutines-naive", ("FIXED", ["DEFAULT", "FIXED", "IO", "VIRTUAL"])),
    ):
        profiles[name] = {
            "spring_profile": spring_profile,
            "default_processing_dispatcher": dispatcher[0],
            "allowed_processing_dispatchers": dispatcher[1],
            "topics": copy.deepcopy(TOPIC_DEFAULTS),
        }
    for name, spring_profile in (
        ("spring-kafka", "spring-kafka"),
        ("spring-kafka-thread-pool", "spring-kafka-thread-pool"),
        ("spring-kafka-virtual-thread-pool", "spring-kafka-virtual-thread-pool"),
    ):
        profiles[name] = {
            "spring_profile": spring_profile,
            "default_processing_dispatcher": "",
            "allowed_processing_dispatchers": [],
            "topics": {
                topic: {
                    "parallelism": ["partitions", "pollers"],
                    "allowed_processing_modes": [
                        "AT_LEAST_ONCE_PARTITION_ORDERING"
                        if topic != "telemetry"
                        else "HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED"
                    ],
                }
                for topic in TOPIC_DEFAULTS
            },
        }
    return {"topics": planner_topics, "profiles": profiles}
