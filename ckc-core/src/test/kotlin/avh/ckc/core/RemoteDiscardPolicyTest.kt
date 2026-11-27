package avh.ckc.core

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordDropReason.NEW_KEY_QUEUE_FULL
import avh.ckc.core.metrics.RecordDropReason.QUEUE_OVERFLOW
import avh.ckc.core.metrics.RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD
import avh.ckc.core.metrics.RecordDropReason.STALE_AGE
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RemoteDiscardPolicyTest {
    @Test
    fun `each processing mode permits only its own remote discard reasons`() {
        val stale = typedTestRecord(offset = 0, timestamp = System.currentTimeMillis() - 60_000)
        val supported = mapOf(
            ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST to setOf(QUEUE_OVERFLOW, STALE_AGE),
            ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY to
                setOf(REPLACED_BY_NEWER_KEY_RECORD, NEW_KEY_QUEUE_FULL, STALE_AGE)
        )

        for (mode in ProcessingMode.entries) {
            val factory = CoroutinesKafkaConsumerBuilder<String, String>().apply {
                processingMode = mode
                freshnessMaxRecordAge = 30.seconds
                handle {}
            }.runtimeFactory()
            assertEquals(
                supported[mode].orEmpty(),
                RecordDropReason.entries.filter { factory.acceptsRemoteDiscard(stale, it) }.toSet(),
                mode.name
            )
        }
    }

    @Test
    fun `remote stale age uses the captured donor limit and source timestamp`() {
        val builder = CoroutinesKafkaConsumerBuilder<String, String>().apply {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            handle {}
        }
        val withoutAgeLimit = builder.runtimeFactory()
        builder.freshnessMaxRecordAge = 30.seconds
        val withAgeLimit = builder.runtimeFactory()

        val now = System.currentTimeMillis()
        val stale = typedTestRecord(offset = 0, timestamp = now - 60_000)
        assertFalse(withoutAgeLimit.acceptsRemoteDiscard(stale, STALE_AGE))
        assertTrue(withAgeLimit.acceptsRemoteDiscard(stale, STALE_AGE))
        assertFalse(withAgeLimit.acceptsRemoteDiscard(typedTestRecord(offset = 0, timestamp = now), STALE_AGE))
        assertFalse(withAgeLimit.acceptsRemoteDiscard(typedTestRecord(offset = 0, timestamp = -1), STALE_AGE))
    }
}
