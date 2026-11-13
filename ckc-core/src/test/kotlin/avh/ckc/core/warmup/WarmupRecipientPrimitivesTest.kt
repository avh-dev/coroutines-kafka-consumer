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
            RecordProcessingOutcome.HandlerSucceededRemotely,
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

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `switchable data plane distributes records fairly across independent sessions`() = runBlocking {
        val switch = SwitchableWarmupDataPlane<String, String>()
        val counts = linkedMapOf("first" to AtomicInteger(), "second" to AtomicInteger())
        fun plane(name: String, resultOffset: Long): FixedLinkWarmupDataPlane<String, String> {
            lateinit var created: FixedLinkWarmupDataPlane<String, String>
            created = FixedLinkWarmupDataPlane(
                sessionId = WarmupSessionId(name),
                quota = 10,
                quotaInterval = 1.seconds,
                delegationTimeout = 1.seconds,
                destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup", 0) },
                sender = WarmupRecordSender { _, _ ->
                    val sequence = counts.getValue(name).incrementAndGet()
                    WarmupSendAcknowledgement(partition, resultOffset + sequence)
                },
                forwardingObserver = WarmupForwardingObserver { event ->
                    created.accept(completed(partition, event.destinationOffset, WarmupSessionId(name)))
                },
                metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
            )
            return created
        }
        switch.activate(plane("first", 10))
        switch.activate(plane("second", 20))

        repeat(6) { offset ->
            switch.process(ConsumerRecord("source", 0, offset.toLong(), "key", "value")) {
                error("local fallback not expected")
            }
        }

        assertEquals(3, counts.getValue("first").get())
        assertEquals(3, counts.getValue("second").get())
        switch.finish(WarmupSessionId("first"))
        repeat(2) { offset ->
            switch.process(ConsumerRecord("source", 0, 10L + offset, "key", "value")) {
                error("remaining session should continue")
            }
        }
        assertEquals(3, counts.getValue("first").get())
        assertEquals(5, counts.getValue("second").get())
        switch.close()
    }

    @Test
    fun `recipient guard rejects records from an old reservation before business processing`() = runBlocking {
        val guard = WarmupRecipientSessionGuard<String, String>(session)
        val localCalls = AtomicInteger()
        val old = ConsumerRecord("warmup", 0, 1L, "key", "value")
        old.headers().add(WARMUP_SESSION_HEADER, "old-session".encodeToByteArray())

        val outcome = guard.process(old) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(0, localCalls.get())
        assertEquals(RecordProcessingOutcome.Filtered, outcome)
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
