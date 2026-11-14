package avh.ckc.core.warmup

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.ProcessingMode
import avh.ckc.core.RetryPolicy
import avh.ckc.core.defaultProcessingRuntime
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingOutcome
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.seconds

class MultiSessionWarmupDataPlaneTest {
    @Test
    fun `pending result in one session does not block delegation through another session`() = runBlocking {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 2, maxAggregateIntervalBudget = 20)
        val firstForwarded = CompletableDeferred<WarmupForwardedEvent>()
        val secondForwarded = CompletableDeferred<WarmupForwardedEvent>()
        val first = WarmupDonorSession(
            sessionId = WarmupSessionId("first-pending"),
            intervalBudget = 10,
            quotaInterval = 1.seconds,
            delegationTimeout = 5.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 1)
            },
            observer = forwardingObserver(firstForwarded)
        )
        val second = WarmupDonorSession(
            sessionId = WarmupSessionId("second-ready"),
            intervalBudget = 10,
            quotaInterval = 1.seconds,
            delegationTimeout = 5.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 2)
            },
            observer = forwardingObserver(secondForwarded)
        )
        plane.admit(first)
        plane.admit(second)
        plane.activate(first.sessionId)
        plane.activate(second.sessionId)

        val firstResult = async { plane.process(record(1)) { error("fallback not expected") } }
        val firstEvent = firstForwarded.await()
        assertFalse(firstResult.isCompleted)

        val secondResult = async { plane.process(record(2)) { error("fallback not expected") } }
        val secondEvent = secondForwarded.await()
        assertEquals(WarmupResultTracker.Completion.DELIVERED, plane.accept(secondEvent.completed()))
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, secondResult.await())
        assertFalse(firstResult.isCompleted)

        assertEquals(WarmupResultTracker.Completion.DELIVERED, plane.accept(firstEvent.completed()))
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, firstResult.await())
        plane.close()
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `remote wait keeps the partition ordering slot occupied`() = runBlocking {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 1, maxAggregateIntervalBudget = 10)
        val events = CopyOnWriteArrayList<WarmupForwardedEvent>()
        val sends = AtomicInteger()
        val session = WarmupDonorSession(
            sessionId = WarmupSessionId("ordered"),
            intervalBudget = 10,
            quotaInterval = 1.seconds,
            delegationTimeout = 5.seconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(
                    TopicPartition(destination.topic, destination.partition),
                    sends.getAndIncrement().toLong()
                )
            },
            observer = object : WarmupDataPlaneObserver<String, String> {
                override fun onForwarded(event: WarmupForwardedEvent, durationNanos: Long) {
                    events += event
                }
            }
        )
        plane.admit(session)
        plane.activate(session.sessionId)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runtime = defaultProcessingRuntime(
            parentScope = scope,
            processingMode = ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING,
            workerConcurrency = 2,
            workChannelCapacity = 4,
            freshnessMaxRecordAge = null,
            processingDispatcher = Dispatchers.Default,
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
            handler = KafkaRecordHandler { error("local handler must not run") },
            retryPolicy = RetryPolicy.none(),
            recordProcessingContext = null,
            processingFailureHandler = ProcessingFailureHandler.skip(),
            processedRecordTracker = NoopProcessedRecordTracker,
            interceptor = plane
        )
        runtime.start { throw it }
        assertTrue(runtime.tryEmit(record(1)))
        assertTrue(runtime.tryEmit(record(2)))
        withTimeout(2_000) { while (events.size != 1) kotlinx.coroutines.yield() }
        assertEquals(1, sends.get())

        plane.accept(events[0].completed())
        withTimeout(2_000) { while (events.size != 2) kotlinx.coroutines.yield() }
        plane.accept(events[1].completed())

        runtime.stop()
        scope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        assertEquals(2, sends.get())
        plane.close()
    }

    @Test
    fun `one donor selects two active sessions fairly and delegates each record once`() = runBlocking {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 2, maxAggregateIntervalBudget = 20)
        val firstSends = AtomicInteger()
        val secondSends = AtomicInteger()
        val localCalls = AtomicInteger()
        val first = successfulSession("first", firstSends, plane)
        val second = successfulSession("second", secondSends, plane)
        assertEquals(MultiSessionWarmupDataPlane.Admission.ACCEPTED, plane.admit(first))
        assertEquals(MultiSessionWarmupDataPlane.Admission.ACCEPTED, plane.admit(second))
        assertTrue(plane.activate(first.sessionId))
        assertTrue(plane.activate(second.sessionId))

        repeat(8) { offset ->
            val outcome = plane.process(record(offset.toLong())) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
            assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, outcome)
        }

        assertEquals(4, firstSends.get())
        assertEquals(4, secondSends.get())
        assertEquals(8, firstSends.get() + secondSends.get())
        assertEquals(0, localCalls.get())
        plane.close()
    }

    @Test
    fun `admission checks session and aggregate budget limits atomically`() = runBlocking {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 3, maxAggregateIntervalBudget = 10)
        val candidates = List(20) { index -> idleSession("session-$index", intervalBudget = 4) }

        val results = candidates.map { session ->
            async(Dispatchers.Default) { session to plane.admit(session) }
        }.map { it.await() }

        assertEquals(2, results.count { it.second == MultiSessionWarmupDataPlane.Admission.ACCEPTED })
        assertEquals(18, results.count { it.second == MultiSessionWarmupDataPlane.Admission.BUDGET_LIMIT })
        assertEquals(2, plane.admittedSessionCount())
        assertEquals(8, plane.admittedIntervalBudget())
        results.filter { it.second != MultiSessionWarmupDataPlane.Admission.ACCEPTED }
            .forEach { it.first.close() }
        plane.close()
    }

    @Test
    fun `failure in one session falls back locally and leaves sibling active`() = runBlocking {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 2, maxAggregateIntervalBudget = 20)
        val failedSends = AtomicInteger()
        val successfulSends = AtomicInteger()
        val localCalls = AtomicInteger()
        val failed = idleSession(
            id = "failed",
            sender = WarmupRecordSender { _, _, _ ->
                failedSends.incrementAndGet()
                throw IllegalStateException("send failed")
            }
        )
        val successful = successfulSession("healthy", successfulSends, plane)
        plane.admit(failed)
        plane.admit(successful)
        plane.activate(failed.sessionId)
        plane.activate(successful.sessionId)

        val fallback = plane.process(record(1)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }
        val remote = plane.process(record(2)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, fallback)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, remote)
        assertEquals(1, failedSends.get())
        assertEquals(1, successfulSends.get())
        assertEquals(1, localCalls.get())
        assertFalse(failed.isActive())
        assertTrue(successful.isActive())
        plane.close()
    }

    @Test
    fun `cancellation while waiting for a result never invokes local fallback`() = runBlocking {
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 1, maxAggregateIntervalBudget = 10)
        val sent = CompletableDeferred<Unit>()
        val localCalls = AtomicInteger()
        val session = idleSession(
            id = "cancelled",
            sender = WarmupRecordSender { _, destination, _ ->
                sent.complete(Unit)
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 1)
            }
        )
        plane.admit(session)
        plane.activate(session.sessionId)
        val processing = async {
            plane.process(record(1)) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        sent.await()

        processing.cancelAndJoin()
        plane.stopDelegatingAndAwaitInflight(session.sessionId, 1.seconds)

        assertEquals(0, localCalls.get())
        assertEquals(0, session.pendingCount())
        plane.finish(session.sessionId)
        Unit
    }

    private fun successfulSession(
        id: String,
        sends: AtomicInteger,
        plane: MultiSessionWarmupDataPlane<String, String>
    ): WarmupDonorSession<String, String> {
        val sessionId = WarmupSessionId(id)
        return idleSession(
            id = id,
            sender = WarmupRecordSender { _, destination, actualSessionId ->
                val offset = sends.getAndIncrement().toLong()
                val topicPartition = TopicPartition(destination.topic, destination.partition)
                plane.accept(
                    WarmupRecordCompleted(
                        WarmupRecordId(actualSessionId, topicPartition, offset),
                        WarmupRemoteOutcome.Succeeded
                    )
                )
                WarmupSendAcknowledgement(topicPartition, offset)
            }
        ).also { assertEquals(sessionId, it.sessionId) }
    }

    private fun idleSession(
        id: String,
        intervalBudget: Int = 10,
        sender: WarmupRecordSender<String, String> = WarmupRecordSender { _, destination, _ ->
            WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 0)
        }
    ): WarmupDonorSession<String, String> = WarmupDonorSession(
        sessionId = WarmupSessionId(id),
        intervalBudget = intervalBudget,
        quotaInterval = 1.seconds,
        delegationTimeout = 5.seconds,
        destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
        sender = sender
    )

    private fun record(offset: Long): ConsumerRecord<String, String> =
        ConsumerRecord("source", 0, offset, "key-$offset", "value-$offset")

    private fun forwardingObserver(target: CompletableDeferred<WarmupForwardedEvent>) =
        object : WarmupDataPlaneObserver<String, String> {
            override fun onForwarded(event: WarmupForwardedEvent, durationNanos: Long) {
                target.complete(event)
            }
        }

    private fun WarmupForwardedEvent.completed() = WarmupRecordCompleted(
        WarmupRecordId(sessionId, destinationTopicPartition, destinationOffset),
        WarmupRemoteOutcome.Succeeded
    )
}
