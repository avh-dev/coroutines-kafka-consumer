package avh.ckc.core.warmup

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.ProcessingMode
import avh.ckc.core.RetryPolicy
import avh.ckc.core.awaitFor
import avh.ckc.core.defaultProcessingRuntime
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.WarmupFallbackReason
import avh.ckc.core.metrics.WarmupResultMetricOutcome
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.core.processing.NoopProcessedRecordTracker
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class FixedLinkWarmupDataPlaneTest {
    private val session = WarmupSessionId("session-1")
    private val destination = WarmupDestination("warmup-orders", 3)

    @Test
    fun `remote success completes source without local handler and emits forwarding after ack`() = runBlocking {
        val metrics = TestMetrics()
        val forwarded = mutableListOf<WarmupForwardedEvent>()
        val localCalls = AtomicInteger()
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(
            metrics = metrics,
            observer = WarmupForwardingObserver { event ->
                forwarded += event
                plane.accept(completed(event, WarmupRemoteOutcome.Succeeded))
            }
        )

        val outcome = plane.process(record(offset = 7L)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, outcome)
        assertEquals(0, localCalls.get())
        assertEquals(1, forwarded.size)
        assertEquals(7L, forwarded.single().sourceOffset)
        assertEquals(11L, forwarded.single().destinationOffset)
        assertEquals(listOf(WarmupResultMetricOutcome.SUCCEEDED), metrics.results)
        assertTrue(metrics.pending.isEmpty())
    }

    @Test
    fun `remote terminal failure runs complete local pipeline`() = runBlocking {
        val metrics = TestMetrics()
        val localCalls = AtomicInteger()
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(metrics = metrics, observer = WarmupForwardingObserver { event ->
            plane.accept(completed(event, WarmupRemoteOutcome.TerminalFailure))
        })

        val outcome = plane.process(record()) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerFailed(IllegalStateException("local failed"))
        }

        assertInstanceOf(RecordProcessingOutcome.HandlerFailed::class.java, outcome)
        assertEquals(1, localCalls.get())
        assertEquals(listOf(WarmupFallbackReason.REMOTE_TERMINAL_FAILURE), metrics.fallbacks)
    }

    @Test
    fun `local pipeline failure after remote terminal failure is not retried as delegation failure`() {
        val localCalls = AtomicInteger()
        val senderCalls = AtomicInteger()
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(
            sender = WarmupRecordSender { _, _ -> acknowledgement(senderCalls.getAndIncrement().toLong()) },
            observer = WarmupForwardingObserver { event ->
                plane.accept(
                    completed(
                        event,
                        if (event.sourceOffset == 1L) {
                            WarmupRemoteOutcome.TerminalFailure
                        } else {
                            WarmupRemoteOutcome.Succeeded
                        }
                    )
                )
            }
        )

        assertThrows(IllegalStateException::class.java) {
            runBlocking {
                plane.process(record(offset = 1L)) {
                    localCalls.incrementAndGet()
                    throw IllegalStateException("local pipeline failed")
                }
            }
        }
        runBlocking {
            assertEquals(
                RecordProcessingOutcome.HandlerSucceeded,
                plane.process(record(offset = 2L)) { error("fallback not expected") }
            )
        }

        assertEquals(1, localCalls.get())
        assertEquals(2, senderCalls.get())
    }

    @Test
    fun `remote discard stays distinct and does not run local handler`() = runBlocking {
        val localCalls = AtomicInteger()
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(supportsRemoteDiscard = true, observer = WarmupForwardingObserver { event ->
            plane.accept(
                completed(
                    event,
                    WarmupRemoteOutcome.Discarded(avh.ckc.core.metrics.RecordDropReason.STALE_AGE)
                )
            )
        })

        val outcome = plane.process(record()) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(
            RecordProcessingOutcome.PolicyDiscarded(avh.ckc.core.metrics.RecordDropReason.STALE_AGE),
            outcome
        )
        assertEquals(0, localCalls.get())
    }

    @Test
    fun `remote discard unsupported by source mode fails session without local fallback`() {
        val localCalls = AtomicInteger()
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(observer = WarmupForwardingObserver { event ->
            plane.accept(
                completed(
                    event,
                    WarmupRemoteOutcome.Discarded(avh.ckc.core.metrics.RecordDropReason.STALE_AGE)
                )
            )
        })

        assertThrows(InvalidWarmupRemoteOutcomeException::class.java) {
            runBlocking {
                plane.process(record()) {
                    localCalls.incrementAndGet()
                    RecordProcessingOutcome.HandlerSucceeded
                }
            }
        }
        assertEquals(0, localCalls.get())
    }

    @Test
    fun `producer ack without outcome times out falls back and disables later delegation`() = runBlocking {
        val metrics = TestMetrics()
        val senderCalls = AtomicInteger()
        val localCalls = AtomicInteger()
        val forwarded = mutableListOf<WarmupForwardedEvent>()
        val plane = plane(
            timeoutMillis = 30,
            metrics = metrics,
            observer = WarmupForwardingObserver { forwarded += it },
            sender = WarmupRecordSender { _, _ ->
                senderCalls.incrementAndGet()
                acknowledgement()
            }
        )

        assertEquals(
            RecordProcessingOutcome.HandlerSucceeded,
            plane.process(record(offset = 1L)) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        )
        plane.process(record(offset = 2L)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(1, senderCalls.get())
        assertEquals(2, localCalls.get())
        assertEquals(
            listOf(WarmupFallbackReason.RESULT_TIMEOUT, WarmupFallbackReason.SESSION_FAILURE),
            metrics.fallbacks
        )
        assertEquals(
            WarmupResultTracker.Completion.IGNORED,
            plane.accept(completed(forwarded.single(), WarmupRemoteOutcome.Succeeded))
        )
        assertEquals(2, localCalls.get())
    }

    @Test
    fun `send failure falls back and prevents new delegation`() = runBlocking {
        val senderCalls = AtomicInteger()
        val localCalls = AtomicInteger()
        val metrics = TestMetrics()
        val plane = plane(metrics = metrics, sender = WarmupRecordSender { _, _ ->
            senderCalls.incrementAndGet()
            throw IllegalStateException("send failed")
        })

        repeat(2) { offset ->
            plane.process(record(offset = offset.toLong())) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }

        assertEquals(1, senderCalls.get())
        assertEquals(2, localCalls.get())
        assertEquals(WarmupFallbackReason.SEND_FAILURE, metrics.fallbacks.first())
    }

    @Test
    fun `session failure releases every unconfirmed delegation to local fallback`() = runBlocking {
        val events = CopyOnWriteArrayList<WarmupForwardedEvent>()
        val localCalls = AtomicInteger()
        val plane = plane(
            sender = WarmupRecordSender { source, _ ->
                if (source.offset() == 3L) throw IllegalStateException("send failed")
                acknowledgement(source.offset())
            },
            observer = WarmupForwardingObserver { events += it }
        )
        val first = async {
            plane.process(record(offset = 1L)) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        val second = async {
            plane.process(record(offset = 2L)) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        withTimeout(1_000) { while (events.size != 2) kotlinx.coroutines.yield() }

        val failedSend = plane.process(record(offset = 3L)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, failedSend)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, first.await())
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, second.await())
        assertEquals(3, localCalls.get())
    }

    @Test
    fun `fatal JVM error from sender propagates without local fallback`() {
        val localCalls = AtomicInteger()
        val plane = plane(sender = WarmupRecordSender { _, _ -> throw LinkageError("fatal") })

        assertThrows(LinkageError::class.java) {
            runBlocking {
                plane.process(record()) {
                    localCalls.incrementAndGet()
                    RecordProcessingOutcome.HandlerSucceeded
                }
            }
        }

        assertEquals(0, localCalls.get())
    }

    @Test
    fun `monitoring observer failure does not change processing outcome`() = runBlocking {
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(
            sender = WarmupRecordSender { _, _ ->
                val acknowledgement = acknowledgement()
                plane.accept(
                    WarmupRecordCompleted(
                        WarmupRecordId(session, acknowledgement.topicPartition, acknowledgement.offset),
                        WarmupRemoteOutcome.Succeeded
                    )
                )
                acknowledgement
            },
            observer = WarmupForwardingObserver { error("monitoring failed") }
        )

        assertEquals(
            RecordProcessingOutcome.HandlerSucceededRemotely,
            plane.process(record()) { error("fallback not expected") }
        )
    }

    @Test
    fun `lifecycle cancellation does not run local fallback`() = runBlocking {
        val sendEntered = CompletableDeferred<Unit>()
        val never = CompletableDeferred<WarmupSendAcknowledgement>()
        val localCalls = AtomicInteger()
        val plane = plane(sender = WarmupRecordSender { _, _ ->
            sendEntered.complete(Unit)
            never.await()
        })
        val delegated = async {
            plane.process(record()) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        sendEntered.await()

        delegated.cancel()
        assertThrows(kotlinx.coroutines.CancellationException::class.java) {
            runBlocking { delegated.await() }
        }
        assertEquals(0, localCalls.get())
    }

    @Test
    fun `closing data plane cancels pending wait without local fallback`() = runBlocking {
        val forwarded = CompletableDeferred<WarmupForwardedEvent>()
        val localCalls = AtomicInteger()
        val plane = plane(observer = WarmupForwardingObserver { forwarded.complete(it) })
        val delegated = async {
            plane.process(record()) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        val event = withTimeout(1_000) { forwarded.await() }

        plane.close()

        assertTrue(runCatching { delegated.await() }.exceptionOrNull() is kotlinx.coroutines.CancellationException)
        assertEquals(0, localCalls.get())
        assertEquals(
            WarmupResultTracker.Completion.IGNORED,
            plane.accept(completed(event, WarmupRemoteOutcome.Succeeded))
        )
    }

    @Test
    fun `stopping delegation keeps current wait and routes new record locally`() = runBlocking {
        val forwarded = CompletableDeferred<WarmupForwardedEvent>()
        val localCalls = AtomicInteger()
        val plane = plane(observer = WarmupForwardingObserver { forwarded.complete(it) })
        val delegated = async { plane.process(record(offset = 1L)) { error("fallback not expected") } }
        val event = withTimeout(1_000) { forwarded.await() }

        plane.stopDelegating()
        val local = plane.process(record(offset = 2L)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }
        assertFalse(delegated.isCompleted)
        plane.accept(completed(event, WarmupRemoteOutcome.Succeeded))

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, local)
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, delegated.await())
        assertEquals(1, localCalls.get())
    }

    @Test
    fun `close waits beyond producer acknowledgement for remote outcome`() = runBlocking {
        val sendEntered = CompletableDeferred<Unit>()
        val releaseSend = CompletableDeferred<Unit>()
        val event = CompletableDeferred<WarmupForwardedEvent>()
        val plane = plane(
            sender = WarmupRecordSender { _, _ ->
                sendEntered.complete(Unit)
                releaseSend.await()
                acknowledgement(17L)
            },
            observer = WarmupForwardingObserver { event.complete(it) }
        )
        val processing = async { plane.process(record()) { error("fallback not expected") } }
        sendEntered.await()
        val stopping = async { plane.stopDelegatingAndAwaitInflight(1.seconds) }
        assertFalse(stopping.isCompleted)

        releaseSend.complete(Unit)
        event.await()
        assertFalse(stopping.isCompleted)
        assertFalse(processing.isCompleted)
        plane.accept(completed(event.await(), WarmupRemoteOutcome.Succeeded))
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, processing.await())
        stopping.await()
    }

    @Test
    fun `close acknowledgement waits for local fallback to finish`() = runBlocking {
        val forwarded = CompletableDeferred<WarmupForwardedEvent>()
        val fallbackEntered = CompletableDeferred<Unit>()
        val releaseFallback = CompletableDeferred<Unit>()
        val plane = plane(observer = WarmupForwardingObserver { forwarded.complete(it) })
        val processing = async {
            plane.process(record()) {
                fallbackEntered.complete(Unit)
                releaseFallback.await()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        val event = forwarded.await()
        plane.accept(completed(event, WarmupRemoteOutcome.TerminalFailure))
        fallbackEntered.await()

        val stopping = async { plane.stopDelegatingAndAwaitInflight(1.seconds) }
        assertFalse(stopping.isCompleted)

        releaseFallback.complete(Unit)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, processing.await())
        stopping.await()
    }

    @Test
    fun `recipient loss falls back inflight and keeps later records local`() = runBlocking {
        val forwarded = CompletableDeferred<WarmupForwardedEvent>()
        val fallbackEntered = CompletableDeferred<Unit>()
        val releaseFallback = CompletableDeferred<Unit>()
        val localCalls = AtomicInteger()
        val plane = plane(observer = WarmupForwardingObserver { forwarded.complete(it) })
        val processing = async {
            plane.process(record(offset = 1L)) {
                localCalls.incrementAndGet()
                fallbackEntered.complete(Unit)
                releaseFallback.await()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }
        forwarded.await()

        val failing = async {
            plane.failAndAwaitInflight(WarmupRecipientUnavailableException("recipient lost"), 1.seconds)
        }
        fallbackEntered.await()
        val later = plane.process(record(offset = 2L)) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, later)
        assertFalse(failing.isCompleted)
        releaseFallback.complete(Unit)
        assertEquals(RecordProcessingOutcome.HandlerSucceeded, processing.await())
        failing.await()
        assertEquals(2, localCalls.get())
    }

    @Test
    fun `interval quota routes excess records locally`() = runBlocking {
        val senderCalls = AtomicInteger()
        val localCalls = AtomicInteger()
        lateinit var plane: FixedLinkWarmupDataPlane<String, String>
        plane = plane(
            quota = 2,
            sender = WarmupRecordSender { _, _ ->
                val offset = senderCalls.getAndIncrement().toLong()
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), offset)
            },
            observer = WarmupForwardingObserver { event ->
                plane.accept(completed(event, WarmupRemoteOutcome.Succeeded))
            }
        )

        repeat(3) { offset ->
            plane.process(record(offset = offset.toLong())) {
                localCalls.incrementAndGet()
                RecordProcessingOutcome.HandlerSucceeded
            }
        }

        assertEquals(2, senderCalls.get())
        assertEquals(1, localCalls.get())
    }

    @Test
    fun `new interval delegates while previous interval result is still pending`() = runBlocking {
        val now = AtomicLong(0L)
        val events = CopyOnWriteArrayList<WarmupForwardedEvent>()
        val sends = AtomicInteger()
        val plane = plane(
            quota = 1,
            nanoTime = now::get,
            sender = WarmupRecordSender { _, _ -> acknowledgement(sends.getAndIncrement().toLong()) },
            observer = WarmupForwardingObserver { events += it }
        )
        val first = async { plane.process(record(offset = 1L)) { error("fallback not expected") } }
        withTimeout(1_000) { while (events.size != 1) kotlinx.coroutines.yield() }

        now.set(1.seconds.inWholeNanoseconds)
        val second = async { plane.process(record(offset = 2L)) { error("fallback not expected") } }
        withTimeout(1_000) { while (events.size != 2) kotlinx.coroutines.yield() }

        events.forEach { plane.accept(completed(it, WarmupRemoteOutcome.Succeeded)) }
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, first.await())
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, second.await())
        assertEquals(2, sends.get())
    }

    @Test
    fun `early result overflow fails session instead of evicting valid result`() = runBlocking {
        val senderCalls = AtomicInteger()
        val localCalls = AtomicInteger()
        val metrics = TestMetrics()
        val plane = plane(
            maxEarlyResults = 1,
            metrics = metrics,
            sender = WarmupRecordSender { _, _ ->
                senderCalls.incrementAndGet()
                acknowledgement()
            }
        )
        val partition = TopicPartition(destination.topic, destination.partition)
        assertEquals(
            WarmupResultTracker.Completion.STORED_EARLY,
            plane.accept(WarmupRecordCompleted(WarmupRecordId(session, partition, 1L), WarmupRemoteOutcome.Succeeded))
        )
        assertEquals(
            WarmupResultTracker.Completion.OVERFLOW,
            plane.accept(WarmupRecordCompleted(WarmupRecordId(session, partition, 2L), WarmupRemoteOutcome.Succeeded))
        )

        plane.process(record()) {
            localCalls.incrementAndGet()
            RecordProcessingOutcome.HandlerSucceeded
        }

        assertEquals(0, senderCalls.get())
        assertEquals(1, localCalls.get())
        assertEquals(listOf(WarmupFallbackReason.SESSION_FAILURE), metrics.fallbacks)
    }

    @Test
    fun `late outcome completes registered wait`() = runBlocking {
        val forwarded = CompletableDeferred<WarmupForwardedEvent>()
        val metrics = TestMetrics()
        val plane = plane(metrics = metrics, observer = WarmupForwardingObserver { forwarded.complete(it) })
        val result = async { plane.process(record()) { error("fallback not expected") } }
        val event = withTimeout(1_000) { forwarded.await() }
        assertFalse(result.isCompleted)
        val completed = completed(event, WarmupRemoteOutcome.Succeeded)

        assertEquals(
            WarmupResultTracker.Completion.DELIVERED,
            plane.accept(completed)
        )
        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, result.await())
        assertEquals(WarmupResultTracker.Completion.DUPLICATE, plane.accept(completed))
        assertEquals(listOf(WarmupResultMetricOutcome.SUCCEEDED), metrics.results)
        assertEquals(listOf(1, 0), metrics.pending)
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `delegation wait keeps partition ordering slot occupied`() = runBlocking {
        val events = CopyOnWriteArrayList<WarmupForwardedEvent>()
        val senderCalls = AtomicInteger()
        val plane = plane(
            sender = WarmupRecordSender { _, _ -> acknowledgement(senderCalls.getAndIncrement().toLong()) },
            observer = WarmupForwardingObserver { events += it }
        )
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
        assertTrue(runtime.tryEmit(record(offset = 1L)))
        assertTrue(runtime.tryEmit(record(offset = 2L)))
        awaitFor(2_000, 10) { events.singleOrNull() }
        assertEquals(1, senderCalls.get())

        plane.accept(completed(events[0], WarmupRemoteOutcome.Succeeded))
        awaitFor(2_000, 10) { events.takeIf { it.size == 2 } }
        plane.accept(completed(events[1], WarmupRemoteOutcome.Succeeded))

        runtime.stop()
        scope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        assertEquals(2, senderCalls.get())
    }

    private fun plane(
        quota: Int = 10,
        timeoutMillis: Long = 500,
        sender: WarmupRecordSender<String, String> = WarmupRecordSender { _, _ -> acknowledgement() },
        observer: WarmupForwardingObserver = WarmupForwardingObserver { },
        metrics: TestMetrics = TestMetrics(),
        maxEarlyResults: Int = 1_024,
        supportsRemoteDiscard: Boolean = false,
        nanoTime: () -> Long = System::nanoTime
    ) = FixedLinkWarmupDataPlane(
        sessionId = session,
        quota = quota,
        quotaInterval = 1.seconds,
        delegationTimeout = timeoutMillis.milliseconds,
        destinationResolver = WarmupDestinationResolver { destination },
        sender = sender,
        forwardingObserver = observer,
        metrics = metrics,
        maxEarlyResults = maxEarlyResults,
        supportsRemoteDiscard = { supportsRemoteDiscard },
        nanoTime = nanoTime
    )

    private fun acknowledgement(offset: Long = 11L) =
        WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), offset)

    private fun WarmupDestination.topicPartition() = TopicPartition(topic, partition)

    private fun completed(event: WarmupForwardedEvent, outcome: WarmupRemoteOutcome) =
        WarmupRecordCompleted(
            WarmupRecordId(event.sessionId, event.destinationTopicPartition, event.destinationOffset),
            outcome
        )

    private fun record(offset: Long = 7L) = ConsumerRecord("source-orders", 2, offset, "key", "value")

    private class TestMetrics : ConsumerMetrics<String, String> {
        val results = mutableListOf<WarmupResultMetricOutcome>()
        val fallbacks = mutableListOf<WarmupFallbackReason>()
        val pending = mutableListOf<Int>()

        override fun onWarmupResult(
            record: ConsumerRecord<String, String>,
            outcome: WarmupResultMetricOutcome,
            waitDurationNanos: Long
        ) {
            results += outcome
        }

        override fun onWarmupFallback(record: ConsumerRecord<String, String>, reason: WarmupFallbackReason) {
            fallbacks += reason
        }

        override fun onWarmupPendingChanged(pending: Int) {
            this.pending += pending
        }
    }
}
