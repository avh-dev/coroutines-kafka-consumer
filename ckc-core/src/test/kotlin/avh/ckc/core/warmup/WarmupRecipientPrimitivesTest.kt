package avh.ckc.core.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.processing.RecordProcessingOutcome
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class WarmupRecipientPrimitivesTest {
    private val session = WarmupSessionId("session")
    private val partition = TopicPartition("warmup", 0)

    @Test
    fun `completion outbox preserves publication order and flush waits for acknowledged sends`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val releaseFirst = CompletableDeferred<Unit>()
        val sent = CopyOnWriteArrayList<Long>()
        val outbox = WarmupCompletionOutbox(scope, capacity = 4) {
            if (it.recordId.offset == 1L) releaseFirst.await()
            sent += it.recordId.offset
        }
        outbox.publish(completed(partition, 1L))
        outbox.publish(completed(partition, 2L))
        val flushing = async { outbox.flush(1.seconds) }
        assertFalse(flushing.isCompleted)

        releaseFirst.complete(Unit)

        flushing.await()
        assertEquals(listOf(1L, 2L), sent)
        outbox.stop()
        scope.cancel()
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `switchable data plane is local while idle and delegates only during active session`() = runBlocking {
        val switch = SwitchableWarmupDataPlane<String, String>()
        val localCalls = AtomicInteger()
        val source = ConsumerRecord("source", 0, 1L, "key", "value")
        assertEquals(
            RecordProcessingOutcome.HandlerSucceeded,
            switch.process(source) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        )
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = FixedLinkWarmupDataPlane(
            sessionId = session,
            quota = 1,
            quotaInterval = 1.seconds,
            delegationTimeout = 1.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup", 0) },
            sender = WarmupRecordSender { _, _ -> WarmupSendAcknowledgement(partition, 7L) },
            forwardingObserver = WarmupForwardingObserver {
                plane.accept(completed(partition, 7L))
            },
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
        )
        switch.activate(plane)

        assertEquals(
            RecordProcessingOutcome.HandlerSucceeded,
            switch.process(source) { error("local fallback not expected") }
        )
        assertEquals(1, localCalls.get())
        assertEquals(
            WarmupResultTracker.Completion.IGNORED,
            switch.accept(completed(TopicPartition("warmup", 1), 9L, WarmupSessionId("foreign")))
        )
        switch.finish(session)
        assertTrue(
            switch.process(source) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            } is RecordProcessingOutcome.HandlerSucceeded
        )
        assertEquals(2, localCalls.get())
    }

    private fun completed(
        topicPartition: TopicPartition,
        offset: Long,
        resultSession: WarmupSessionId = session
    ) = WarmupRecordCompleted(
        WarmupRecordId(resultSession, topicPartition, offset),
        WarmupRemoteOutcome.Succeeded
    )
}
