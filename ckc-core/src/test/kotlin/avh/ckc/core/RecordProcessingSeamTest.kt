package avh.ckc.core

import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.core.processing.RecordProcessor
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.warmup.MultiSessionWarmupDataPlane
import avh.ckc.core.warmup.WarmupDataPlaneObserver
import avh.ckc.core.warmup.WarmupDestination
import avh.ckc.core.warmup.WarmupDestinationResolver
import avh.ckc.core.warmup.WarmupDonorSession
import avh.ckc.core.warmup.WarmupForwardedEvent
import avh.ckc.core.warmup.WarmupRecordSender
import avh.ckc.core.warmup.WarmupSendAcknowledgement
import avh.ckc.core.warmup.WarmupSessionId
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RecordProcessingSeamTest {
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `ownership revoked while awaiting remote result prevents local fallback handler`() = runBlocking {
        val localHandled = CompletableDeferred<Long>()
        val forwarded = CompletableDeferred<WarmupForwardedEvent>()
        val plane = MultiSessionWarmupDataPlane<String, String>(maxSessions = 1, maxAggregateBudgetPerSecond = 10)
        val session = WarmupDonorSession(
            sessionId = WarmupSessionId("revoked"),
            intervalBudget = 10,
            quotaInterval = 1.seconds,
            delegationTimeout = 100.milliseconds,
            destinationResolver = WarmupDestinationResolver { WarmupDestination("warmup") },
            sender = WarmupRecordSender { _, destination, _ ->
                WarmupSendAcknowledgement(TopicPartition(destination.topic, destination.partition), 1)
            },
            observer = object : WarmupDataPlaneObserver<String, String> {
                override fun onForwarded(
                    record: ConsumerRecord<String, String>,
                    event: WarmupForwardedEvent,
                    durationNanos: Long
                ) {
                    forwarded.complete(event)
                }
            }
        )
        plane.admit(session)
        plane.activate(session.sessionId)
        val partition = TopicPartition("topic-a", 0)
        val registry = PartitionRegistry()
        registry.onPartitionsAssigned(listOf(partition))
        val tracker = PartitionProcessedRecordTracker(registry)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val failure = CompletableDeferred<Throwable>()
        val runtime = defaultProcessingRuntime(
            parentScope = scope,
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            freshnessMaxRecordAge = null,
            processingDispatcher = Dispatchers.Default,
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
            handler = KafkaRecordHandler { localHandled.complete(it.offset()) },
            retryPolicy = RetryPolicy.none(),
            recordProcessingContext = null,
            processingFailureHandler = ProcessingFailureHandler.skip(),
            processedRecordTracker = tracker,
            interceptor = plane
        )
        runtime.start { failure.complete(it) }
        val delegated = typedTestRecord(offset = 19)
        registry.observe(delegated)
        assertTrue(runtime.tryEmit(delegated))
        forwarded.await()

        registry.revoke(listOf(partition))
        plane.stopDelegatingAndAwaitInflight(session.sessionId, 2.seconds)
        assertTrue(plane.removeDrainedSession(session.sessionId))

        registry.onPartitionsAssigned(listOf(partition))
        val local = typedTestRecord(offset = 20)
        registry.observe(local)
        assertTrue(runtime.tryEmit(local))
        assertEquals(20L, withTimeout(2_000) { localHandled.await() })

        assertFalse(failure.isCompleted)
        runtime.stop()
        scope.cancel()
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `every processing mode invokes delegation inside its admitted runtime slot`() = runBlocking {
        ProcessingMode.entries.forEach { mode ->
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val intercepted = CompletableDeferred<Long>()
            val failure = CompletableDeferred<Throwable>()
            val runtime = defaultProcessingRuntime(
                parentScope = scope,
                processingMode = mode,
                workerConcurrency = 2,
                workChannelCapacity = 8,
                freshnessMaxRecordAge = null,
                processingDispatcher = Dispatchers.Default,
                metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
                handler = KafkaRecordHandler { error("local handler must not run") },
                retryPolicy = RetryPolicy.none(),
                recordProcessingContext = null,
                processingFailureHandler = ProcessingFailureHandler.skip(),
                processedRecordTracker = NoopProcessedRecordTracker,
                interceptor = AdmittedRecordInterceptor { record, _ ->
                    intercepted.complete(record.offset())
                    RecordProcessingOutcome.HandlerSucceededRemotely
                }
            )

            runtime.start { failure.complete(it) }
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 7)))
            assertEquals(7L, withTimeout(2_000) { intercepted.await() })
            assertFalse(failure.isCompleted, "mode $mode failed")
            runtime.stop()
            scope.cancel()
        }
    }

    @Test
    fun `remote success bypasses local handler and terminalizes the source record`() = runBlocking {
        val handlerCalls = AtomicInteger()
        val processedCalls = AtomicInteger()
        val metrics = RecordingMetrics<String, String>()
        val processor = RecordProcessor(
            handler = KafkaRecordHandler<String, String> { handlerCalls.incrementAndGet() },
            retryPolicy = RetryPolicy.none(),
            metrics = metrics,
            processingFailureHandler = ProcessingFailureHandler.skip(),
            onRecordProcessed = { processedCalls.incrementAndGet() },
            interceptor = AdmittedRecordInterceptor { _, _ ->
                RecordProcessingOutcome.HandlerSucceededRemotely
            }
        )

        val outcome = processor.process(typedTestRecord(offset = 17))

        assertEquals(RecordProcessingOutcome.HandlerSucceededRemotely, outcome)
        assertEquals(0, handlerCalls.get())
        assertEquals(1, processedCalls.get())
        assertEquals(1, metrics.processed.size)
    }

    @Test
    fun `interceptor may invoke the complete local retry pipeline as fallback`() = runBlocking {
        val attempts = AtomicInteger()
        val processor = RecordProcessor(
            handler = KafkaRecordHandler<String, String> {
                if (attempts.getAndIncrement() == 0) error("retry")
            },
            retryPolicy = retryPolicy { retry<IllegalStateException> { maxRetries = 1 } },
            metrics = RecordingMetrics(),
            processingFailureHandler = ProcessingFailureHandler.skip(),
            onRecordProcessed = {},
            interceptor = AdmittedRecordInterceptor { _, processLocally -> processLocally() }
        )

        val outcome = processor.process(typedTestRecord(offset = 18))

        assertEquals(RecordProcessingOutcome.HandlerSucceeded, outcome)
        assertEquals(2, attempts.get())
    }
}
