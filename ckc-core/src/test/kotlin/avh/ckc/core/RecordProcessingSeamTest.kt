package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.AdmittedRecordInterceptor
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.RecordProcessingOutcome
import avh.ckc.core.processing.RecordProcessingOutcomeObserver
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class RecordProcessingSeamTest {

    @Test
    fun `every processing mode invokes the admitted record interceptor inside its runtime slot`() = runBlocking {
        ProcessingMode.entries.forEach { mode ->
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val intercepted = CompletableDeferred<Long>()
            val observed = CompletableDeferred<RecordProcessingOutcome>()
            val failure = CompletableDeferred<Throwable>()
            val runtime = runtime(
                mode = mode,
                scope = scope,
                interceptor = AdmittedRecordInterceptor { record, _ ->
                    intercepted.complete(record.offset())
                    RecordProcessingOutcome.HandlerSucceeded
                },
                observer = RecordProcessingOutcomeObserver { _, outcome -> observed.complete(outcome) }
            )

            runtime.start { failure.complete(it) }
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 7L)))

            assertEquals(7L, withTimeout(2_000) { intercepted.await() })
            assertEquals(RecordProcessingOutcome.HandlerSucceeded, withTimeout(2_000) { observed.await() })
            assertFalse(failure.isCompleted, "mode $mode failed")
            runtime.stop()
            scope.cancel()
        }
    }

    @Test
    fun `freshness policy discard is observed without invoking admitted interceptor`() = runBlocking {
        listOf(
            ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
            ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
        ).forEach { mode ->
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val intercepted = CompletableDeferred<Unit>()
            val observed = CompletableDeferred<RecordProcessingOutcome>()
            val failure = CompletableDeferred<Throwable>()
            val runtime = runtime(
                mode = mode,
                scope = scope,
                freshnessMaxRecordAge = 1.milliseconds,
                interceptor = AdmittedRecordInterceptor { _, local ->
                    intercepted.complete(Unit)
                    local()
                },
                observer = RecordProcessingOutcomeObserver { _, outcome -> observed.complete(outcome) }
            )

            runtime.start { failure.complete(it) }
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 8L, timestamp = 1L)))

            assertEquals(
                RecordProcessingOutcome.PolicyDiscarded(RecordDropReason.STALE_AGE),
                withTimeout(2_000) { observed.await() }
            )
            assertFalse(intercepted.isCompleted)
            assertFalse(failure.isCompleted, "mode $mode failed")
            runtime.stop()
            scope.cancel()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun runtime(
        mode: ProcessingMode,
        scope: CoroutineScope,
        freshnessMaxRecordAge: kotlin.time.Duration? = null,
        interceptor: AdmittedRecordInterceptor<String, String>,
        observer: RecordProcessingOutcomeObserver<String, String>
    ) = defaultProcessingRuntime(
        parentScope = scope,
        processingMode = mode,
        workerConcurrency = 2,
        workChannelCapacity = 8,
        freshnessMaxRecordAge = freshnessMaxRecordAge,
        processingDispatcher = Dispatchers.Default,
        metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
        handler = KafkaRecordHandler { error("local handler must not run") },
        retryPolicy = RetryPolicy.none(),
        recordProcessingContext = null,
        processingFailureHandler = ProcessingFailureHandler.skip(),
        processedRecordTracker = NoopProcessedRecordTracker,
        interceptor = interceptor,
        outcomeObserver = observer
    )
}
