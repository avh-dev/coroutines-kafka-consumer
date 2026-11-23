package avh.ckc.warmup

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.warmup.protocol.WarmupDiscardReason

internal fun RecordDropReason.toProtocol(): WarmupDiscardReason = when (this) {
    RecordDropReason.ALREADY_PROCESSED -> WarmupDiscardReason.ALREADY_PROCESSED
    RecordDropReason.QUEUE_OVERFLOW -> WarmupDiscardReason.QUEUE_OVERFLOW
    RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD -> WarmupDiscardReason.REPLACED_BY_NEWER_KEY_RECORD
    RecordDropReason.NEW_KEY_QUEUE_FULL -> WarmupDiscardReason.NEW_KEY_QUEUE_FULL
    RecordDropReason.STALE_AGE -> WarmupDiscardReason.STALE_AGE
    RecordDropReason.LIFECYCLE_DISCARD -> WarmupDiscardReason.LIFECYCLE_DISCARD
}

internal fun WarmupDiscardReason.toCore(): RecordDropReason = when (this) {
    WarmupDiscardReason.ALREADY_PROCESSED -> RecordDropReason.ALREADY_PROCESSED
    WarmupDiscardReason.QUEUE_OVERFLOW -> RecordDropReason.QUEUE_OVERFLOW
    WarmupDiscardReason.REPLACED_BY_NEWER_KEY_RECORD -> RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD
    WarmupDiscardReason.NEW_KEY_QUEUE_FULL -> RecordDropReason.NEW_KEY_QUEUE_FULL
    WarmupDiscardReason.STALE_AGE -> RecordDropReason.STALE_AGE
    WarmupDiscardReason.LIFECYCLE_DISCARD -> RecordDropReason.LIFECYCLE_DISCARD
}
