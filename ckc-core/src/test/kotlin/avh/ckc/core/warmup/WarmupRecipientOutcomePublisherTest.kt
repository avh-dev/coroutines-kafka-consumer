package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.processing.RecordProcessingOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WarmupRecipientOutcomePublisherTest {
    @Test
    fun `recipient publishes only terminal outcomes with destination coordinates`() {
        val published = mutableListOf<WarmupRecordCompleted>()
        val session = WarmupSessionId("session-1")
        val observer = WarmupRecipientOutcomePublisher<String, String>(session) { published += it }
        val record = ConsumerRecord("warmup-orders", 3, 11L, "key", "value")

        observer.onOutcome(record, RecordProcessingOutcome.HandlerSucceeded)
        observer.onOutcome(record, RecordProcessingOutcome.HandlerFailed(IllegalStateException("failed")))
        observer.onOutcome(record, RecordProcessingOutcome.PolicyDiscarded(RecordDropReason.STALE_AGE))
        observer.onOutcome(record, RecordProcessingOutcome.HandedOff(RecordHandoffReason.ASSIGNMENT_ENDED))

        assertEquals(3, published.size)
        assertEquals(
            List(3) { WarmupRecordId(session, TopicPartition("warmup-orders", 3), 11L) },
            published.map { it.recordId }
        )
        assertEquals(
            listOf(
                WarmupRemoteOutcome.Succeeded,
                WarmupRemoteOutcome.TerminalFailure,
                WarmupRemoteOutcome.Discarded(RecordDropReason.STALE_AGE)
            ),
            published.map { it.outcome }
        )
    }
}
