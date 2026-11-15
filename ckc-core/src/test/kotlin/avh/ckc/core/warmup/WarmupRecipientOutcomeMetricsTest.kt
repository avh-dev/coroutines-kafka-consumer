package avh.ckc.core.warmup

import avh.ckc.core.RecordingMetrics
import avh.ckc.core.metrics.RecordDropReason
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class WarmupRecipientOutcomeMetricsTest {
    @Test
    fun `recipient publishes terminal runtime outcomes with its session coordinate`() {
        val published = mutableListOf<WarmupRecordCompleted>()
        val delegate = RecordingMetrics<String, String>()
        val metrics = WarmupRecipientOutcomeMetrics(
            WarmupSessionId("session"),
            WarmupCompletionPublisher(published::add),
            delegate
        )
        val record = ConsumerRecord("warmup-orders", 0, 11L, "key", "value")

        metrics.onRecordProcessed("key", "value", record, 1, 2, processedRemotely = false)
        metrics.onRecordFailed("key", "value", record, IllegalStateException("failed"), 3)
        metrics.onRecordDropped(record, RecordDropReason.STALE_AGE)

        assertEquals(3, published.size)
        assertEquals(
            List(3) { WarmupRecordId(WarmupSessionId("session"), TopicPartition("warmup-orders", 0), 11) },
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
        assertEquals(1, delegate.processed.size)
        assertEquals(1, delegate.failed.size)
        assertEquals(1, delegate.dropped.size)
    }

    @Test
    fun `completion outbox preserves order and flush waits for transport acknowledgement`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val releaseFirst = CompletableDeferred<Unit>()
        val sent = mutableListOf<WarmupRecordCompleted>()
        val outbox = WarmupCompletionOutbox(scope, capacity = 4) { completed ->
            if (sent.isEmpty()) releaseFirst.await()
            sent += completed
        }
        val first = completed(offset = 1)
        val second = completed(offset = 2)
        outbox.publish(first)
        outbox.publish(second)

        val flush = async { outbox.flush(2.seconds) }
        delay(50)
        assertEquals(false, flush.isCompleted)
        releaseFirst.complete(Unit)
        flush.await()

        assertEquals(listOf(first, second), sent)
        outbox.stop()
        scope.cancel()
    }

    private fun completed(offset: Long) = WarmupRecordCompleted(
        WarmupRecordId(
            WarmupSessionId("session"),
            TopicPartition("warmup-orders", 0),
            offset
        ),
        WarmupRemoteOutcome.Succeeded
    )
}
