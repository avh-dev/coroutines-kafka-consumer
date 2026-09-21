from __future__ import annotations


TOPIC_PALETTE = {
    "order.events.v1": ("#bfdbfe", "#60a5fa", "#1e3a8a"),
    "batch.events.v1": ("#ddd6fe", "#a78bfa", "#4c1d95"),
    "cauldron.events.v1": ("#ccfbf1", "#2dd4bf", "#134e4a"),
}
FALLBACK_TOPIC_PALETTE = ("#fde68a", "#fbbf24", "#78350f")


def topic_palette(topic: object) -> tuple[str, str, str]:
    return TOPIC_PALETTE.get(str(topic), FALLBACK_TOPIC_PALETTE)
