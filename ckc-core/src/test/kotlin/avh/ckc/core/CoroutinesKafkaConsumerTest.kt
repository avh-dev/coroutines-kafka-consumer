package avh.ckc.core

import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.polling.ConsumerPollLoopControl
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.processing.runtime.AtLeastOnceOrderingRecordProcessingRuntime
import avh.ckc.core.processing.runtime.AtLeastOnceNoOrderingRecordProcessingRuntime
import avh.ckc.core.processing.runtime.FreshnessFirstReplacePendingByKeyRecordProcessingRuntime
import avh.ckc.core.processing.runtime.FreshnessFirstDropOldestRecordProcessingRuntime
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration.Companion.seconds

class CoroutinesKafkaConsumerTest {

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `when auto commit is omitted then poll loop receives it disabled`() {
        lateinit var receivedProperties: Map<String, Any?>

        CoroutinesKafkaConsumer<String, String>(
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            workerConcurrency = 1,
            consumerPollLoopConcurrency = 1,
            commitIntervalMs = 1_000L,
            commitRecordsThreshold = 1_000,
            workChannelCapacity = 16,
            freshnessMaxRecordAge = null,
            processingDispatcher = Dispatchers.Default,
            consumerProperties = stringSerdeProperties(),
            handler = KafkaRecordHandler { },
            retryPolicy = RetryPolicy.none(),
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
            recordProcessingContext = null,
            processingFailureHandler = ProcessingFailureHandler.skip(),
            parentContext = EmptyCoroutineContext,
            topics = listOf("topic-a"),
            topicsPattern = null,
            pollLoopFactory = { _, _, _, _, _, _, properties, _, _, _, _, _ ->
                receivedProperties = properties
                object : ConsumerPollLoopControl {
                    override fun start() = error("not started")
                    override fun prepareForShutdown() = CompletableDeferred(Unit)
                    override fun stateSnapshot() = error("not started")
                }
            },
            processingRuntimeFactory = ::defaultProcessingRuntime
        )

        assertEquals(false, receivedProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
    }

    @Test
    fun `when processing mode is AT_LEAST_ONCE_NO_ORDERING then default factory creates at least once runtime`() {
        val runtime = createDefaultProcessingRuntime(ProcessingMode.AT_LEAST_ONCE_NO_ORDERING)

        assertInstanceOf(AtLeastOnceNoOrderingRecordProcessingRuntime::class.java, runtime)
    }

    @Test
    fun `when processing mode is AT_LEAST_ONCE_KEY_ORDERING then default factory creates ordered runtime`() {
        val runtime = createDefaultProcessingRuntime(ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING)

        assertInstanceOf(AtLeastOnceOrderingRecordProcessingRuntime::class.java, runtime)
    }

    @Test
    fun `when processing mode is AT_LEAST_ONCE_PARTITION_ORDERING then default factory creates ordered runtime`() {
        val runtime = createDefaultProcessingRuntime(ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING)

        assertInstanceOf(AtLeastOnceOrderingRecordProcessingRuntime::class.java, runtime)
    }

    @Test
    fun `when processing mode is FRESHNESS_FIRST_DROP_OLDEST then default factory creates freshness first runtime`() {
        val runtime = createDefaultProcessingRuntime(ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST)

        assertInstanceOf(FreshnessFirstDropOldestRecordProcessingRuntime::class.java, runtime)
    }

    @Test
    fun `when processing mode is FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY then default factory creates freshness first by key runtime`() {
        val runtime = createDefaultProcessingRuntime(ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY)

        assertInstanceOf(FreshnessFirstReplacePendingByKeyRecordProcessingRuntime::class.java, runtime)
    }

    @Test
    fun `when freshness-first queue overflows then dropped record is reported to metrics`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val firstRecordStarted = CompletableDeferred<Unit>()
        val releaseFirstRecord = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(
                typedTestRecord(offset = 1L, key = "key-1", value = "value-1"),
                typedTestRecord(offset = 2L, key = "key-2", value = "value-2"),
                typedTestRecord(offset = 3L, key = "key-3", value = "value-3")
            ),
            consumerProperties = stringSerdeProperties(),
            metrics = metrics,
            runtime = testRuntime(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 1
            ),
            handler = KafkaRecordHandler<String, String> { record ->
                if (record.offset() == 1L) {
                    firstRecordStarted.complete(Unit)
                    releaseFirstRecord.await()
                }
            }
        )

        consumer.start()
        withTimeout(2_000) { firstRecordStarted.await() }
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            metrics.dropped.singleOrNull()
        }

        releaseFirstRecord.complete(Unit)
        consumer.stop()

        assertEquals(listOf(2L), metrics.dropped.map { it.record.offset() })
    }

    @Test
    fun `when freshness-first record is older than max age then handler is not invoked`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val handled = mutableListOf<Long>()
        val consumer = createTestConsumer(
            records = listOf(
                typedTestRecord(
                    offset = 1L,
                    timestamp = System.currentTimeMillis() - 20_000L,
                    key = "key-1",
                    value = "value-1"
                )
            ),
            consumerProperties = stringSerdeProperties(),
            metrics = metrics,
            runtime = testRuntime(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                freshnessMaxRecordAge = 10.seconds
            ),
            handler = KafkaRecordHandler<String, String> { record ->
                handled += record.offset()
            }
        )

        consumer.start()
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            metrics.dropped.singleOrNull()
        }
        consumer.stop()

        assertEquals(emptyList<Long>(), handled)
        assertEquals(listOf(1L), metrics.dropped.map { it.record.offset() })
        assertEquals(listOf(avh.ckc.core.metrics.RecordDropReason.STALE_AGE), metrics.dropped.map { it.reason })
    }

    @Test
    fun `when freshness-first consumer stops then queued records are not reported as dropped`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val firstRecordStarted = CompletableDeferred<Unit>()
        val releaseFirstRecord = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(
                typedTestRecord(offset = 1L, key = "key-1", value = "value-1"),
                typedTestRecord(offset = 2L, key = "key-2", value = "value-2")
            ),
            consumerProperties = stringSerdeProperties(),
            metrics = metrics,
            runtime = testRuntime(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 2
            ),
            handler = KafkaRecordHandler<String, String> { record ->
                if (record.offset() == 1L) {
                    firstRecordStarted.complete(Unit)
                    releaseFirstRecord.await()
                }
            }
        )

        consumer.start()
        withTimeout(2_000) { firstRecordStarted.await() }

        val stopJob = async { consumer.stop() }
        assertFalse(stopJob.isCompleted)
        releaseFirstRecord.complete(Unit)
        withTimeout(2_000) { stopJob.await() }

        assertEquals(emptyList<Long>(), metrics.dropped.map { it.record.offset() })
    }

    @Test
    fun `when record is received then typed record is passed to handler`() = runBlocking {
        val processed = CompletableDeferred<Triple<String?, String?, Long>>()
        val consumer = createTestConsumer(
            records = listOf(typedTestRecord(offset = 10L, key = "key-10", value = "payload")),
            consumerProperties = stringSerdeProperties(),
            handler = KafkaRecordHandler<String, String> { record ->
                processed.complete(Triple(record.key(), record.value(), record.offset()))
            }
        )

        consumer.start()

        assertEquals(Triple("key-10", "payload", 10L), withTimeout(2_000) { processed.await() })
        consumer.stop()
    }

    @Test
    fun `when stop called then it waits for inflight worker completion`() = runBlocking {
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(ConsumerRecord("topic-a", 0, 11L, 0L, 0L)),
            consumerProperties = longSerdeProperties(),
            handler = KafkaRecordHandler<Long, Long> {
                started.complete(Unit)
                release.await()
            }
        )

        consumer.start()
        withTimeout(2_000) { started.await() }

        val stopJob = async { consumer.stop() }
        assertFalse(stopJob.isCompleted)

        release.complete(Unit)
        withTimeout(2_000) { stopJob.await() }
    }

    @Test
    fun `when parent is cancelled during stop then shutdown completion waits for handler cleanup`() = runBlocking {
        val parentJob = Job()
        val handlerStarted = CompletableDeferred<Unit>()
        val cleanupStarted = CompletableDeferred<Unit>()
        val releaseCleanup = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(typedTestRecord(offset = 12L)),
            consumerProperties = stringSerdeProperties(),
            parentContext = parentJob + Dispatchers.Default,
            handler = KafkaRecordHandler<String, String> {
                try {
                    handlerStarted.complete(Unit)
                    awaitCancellation()
                } finally {
                    cleanupStarted.complete(Unit)
                    withContext(NonCancellable) {
                        releaseCleanup.await()
                    }
                }
            }
        )

        consumer.start()
        withTimeout(2_000) { handlerStarted.await() }
        val stopJob = async { runCatching { consumer.stop() }.exceptionOrNull() }
        val shutdownCompletionJob = async { consumer.awaitShutdownCompletion() }

        try {
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                consumer.stateSnapshot().takeIf { it.stopped }
            }
            parentJob.cancel()
            withTimeout(2_000) { cleanupStarted.await() }

            assertFalse(shutdownCompletionJob.isCompleted)

            releaseCleanup.complete(Unit)
            withTimeout(2_000) { shutdownCompletionJob.await() }
            assertInstanceOf(
                kotlinx.coroutines.CancellationException::class.java,
                withTimeout(2_000) { stopJob.await() }
            )
        } finally {
            releaseCleanup.complete(Unit)
            parentJob.cancel()
        }
        Unit
    }

    @Test
    fun `when consumer stops repeatedly then every poll loop resource is released once`() = runBlocking {
        val parentJob = Job()
        val loops = mutableListOf<TrackingPollLoop>()
        val consumer = lifecycleTestConsumer(
            consumerPollLoopConcurrency = 2,
            parentContext = parentJob + Dispatchers.Default
        ) { _, context ->
            TrackingPollLoop(context).also(loops::add)
        }

        consumer.start()
        consumer.stop()
        consumer.stop()

        assertEquals(listOf(1, 1), loops.map { it.releaseCalls.get() })
        assertTrue(loops.all { it.job?.isCompleted == true })
        awaitFor(2_000, 10) { true.takeIf { parentJob.children.none() } }
        assertTrue(parentJob.isActive)
    }

    @Test
    fun `when poll loop startup fails then started and unstarted loop resources are released`() = runBlocking {
        val failure = IllegalStateException("poll loop start failed")
        val parentJob = Job()
        val loops = mutableListOf<TrackingPollLoop>()
        val consumer = lifecycleTestConsumer(
            consumerPollLoopConcurrency = 2,
            parentContext = parentJob + Dispatchers.Default
        ) { id, context ->
            TrackingPollLoop(context, startFailure = failure.takeIf { id == 1 }).also(loops::add)
        }

        val thrown = assertThrows(IllegalStateException::class.java) {
            consumer.start()
        }
        withTimeout(2_000) { consumer.awaitShutdownCompletion() }

        assertSame(failure, thrown)
        assertEquals(listOf(1, 1), loops.map { it.releaseCalls.get() })
        assertTrue(loops.first().job?.isCompleted == true)
        assertEquals(null, loops.last().job)
        awaitFor(2_000, 10) { true.takeIf { parentJob.children.none() } }
        assertTrue(parentJob.isActive)
    }

    @Test
    fun `when poll loop fails terminally then resources are released without explicit stop`() = runBlocking {
        val failure = IllegalStateException("terminal poll failure")
        val parentJob = Job()
        lateinit var loop: TrackingPollLoop
        val consumer = lifecycleTestConsumer(parentContext = parentJob + Dispatchers.Default) { _, context ->
            TrackingPollLoop(context).also { loop = it }
        }
        consumer.start()

        loop.fail(failure)
        withTimeout(2_000) { consumer.awaitShutdownCompletion() }

        assertEquals(1, loop.releaseCalls.get())
        assertTrue(loop.job?.isCompleted == true)
        awaitFor(2_000, 10) { true.takeIf { parentJob.children.none() } }
        assertTrue(parentJob.isActive)
        assertSame(
            failure,
            assertThrows(IllegalStateException::class.java) {
                runBlocking { consumer.awaitTermination() }
            }
        )
    }

    @Test
    fun `when consumer stops then externally owned processing dispatcher remains usable`() = runBlocking {
        val executor = Executors.newSingleThreadExecutor()
        val dispatcher = executor.asCoroutineDispatcher()
        try {
            val consumer = lifecycleTestConsumer(processingDispatcher = dispatcher) { _, context ->
                TrackingPollLoop(context)
            }

            consumer.start()
            consumer.stop()

            assertFalse(executor.isShutdown)
            assertEquals("still-usable", executor.submit<String> { "still-usable" }.get())
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `when processing then handler runs on processing dispatcher`() = runBlocking {
        val processedThread = CompletableDeferred<String>()
        val dispatcher = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "processing-thread")
        }.asCoroutineDispatcher()

        try {
            val consumer = createTestConsumer(
                records = listOf(typedTestRecord(offset = 22L)),
                consumerProperties = stringSerdeProperties(),
                runtime = testRuntime(
                    processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                    processingDispatcher = dispatcher
                ),
                handler = KafkaRecordHandler<String, String> {
                    processedThread.complete(Thread.currentThread().name)
                }
            )

            consumer.start()
            assertTrue(withTimeout(2_000) { processedThread.await() }.contains("processing-thread"))
            consumer.stop()
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `when consumer starts then runtime metrics expose worker and queue stats`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val firstRecordStarted = CompletableDeferred<Unit>()
        val releaseFirstRecord = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(
                typedTestRecord(offset = 31L, key = "key-31", value = "value-31"),
                typedTestRecord(offset = 32L, key = "key-32", value = "value-32")
            ),
            consumerProperties = stringSerdeProperties(),
            metrics = metrics,
            handler = KafkaRecordHandler<String, String> { record ->
                if (record.offset() == 31L) {
                    firstRecordStarted.complete(Unit)
                    releaseFirstRecord.await()
                }
            }
        )

        consumer.start()
        withTimeout(2_000) { firstRecordStarted.await() }

        val stats = metrics.boundRuntimeStats.single()
        assertEquals(1, stats.workerCount)
        assertEquals(1, stats.activeWorkerCount)
        assertEquals(1, stats.workQueueSize)
        assertEquals(1024, stats.workQueueCapacity)
        assertEquals(2, stats.maxObservedWorkQueueSize)

        releaseFirstRecord.complete(Unit)
        consumer.stop()

        assertSame(stats, metrics.boundRuntimeStats.single())
        assertEquals(1, metrics.unbindRuntimeMetricsCalls.size)
    }

    @Test
    fun `when consumer is running then state snapshot exposes runtime counters`() = runBlocking {
        val firstRecordStarted = CompletableDeferred<Unit>()
        val releaseFirstRecord = CompletableDeferred<Unit>()
        val consumer = createTestConsumer(
            records = listOf(
                typedTestRecord(offset = 41L, key = "key-41", value = "value-41"),
                typedTestRecord(offset = 42L, key = "key-42", value = "value-42")
            ),
            consumerProperties = stringSerdeProperties(),
            runtime = testRuntime(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 64
            ),
            handler = KafkaRecordHandler<String, String> { record ->
                if (record.offset() == 41L) {
                    firstRecordStarted.complete(Unit)
                    releaseFirstRecord.await()
                }
            }
        )

        consumer.start()
        withTimeout(2_000) { firstRecordStarted.await() }

        val snapshot = consumer.stateSnapshot()

        assertTrue(snapshot.started)
        assertFalse(snapshot.stopped)
        assertFalse(snapshot.failed)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_NO_ORDERING, snapshot.processingMode)
        assertEquals(1, snapshot.processing.workerCount)
        assertEquals(1, snapshot.processing.activeWorkerCount)
        assertEquals(1, snapshot.processing.workQueueSize)
        assertEquals(64, snapshot.processing.workQueueCapacity)
        assertEquals(1, snapshot.pollLoops.size)
        assertTrue(snapshot.pollLoops.single().started)
        assertEquals(listOf(AssignedPartitionSnapshot("topic-a", 0)), snapshot.pollLoops.single().assignedPartitions)

        releaseFirstRecord.complete(Unit)
        consumer.stop()
    }

    @Test
    fun `when one poll loop fails then terminal shutdown cancels its sibling`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val expected = IllegalStateException("poll loop failed")
        val failFirstLoop = CompletableDeferred<Unit>()
        val siblingStarted = CompletableDeferred<Unit>()
        val siblingCancelled = CompletableDeferred<Unit>()
        val consumer: CoroutinesKafkaConsumer<String, String> = CoroutinesKafkaConsumer(
            processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            workerConcurrency = 1,
            consumerPollLoopConcurrency = 2,
            commitIntervalMs = 1_000L,
            commitRecordsThreshold = 1_000,
            workChannelCapacity = 16,
            freshnessMaxRecordAge = null,
            processingDispatcher = kotlinx.coroutines.Dispatchers.Default,
            consumerProperties = stringSerdeProperties(),
            retryPolicy = RetryPolicy.none(),
            metrics = metrics,
            handler = KafkaRecordHandler<String, String> { },
            recordProcessingContext = null,
            processingFailureHandler = ProcessingFailureHandler.skip<String, String>(),
            parentContext = EmptyCoroutineContext,
            topics = listOf("topic-a"),
            topicsPattern = null,
            pollLoopFactory = { index: Int, context, _: ProcessingMode, _: Long, _: Int, _: ConsumerMetrics<String, String>, _: Map<String, Any?>, _: KafkaConsumerConfigAdapter, _: List<String>?, _: java.util.regex.Pattern?, _: PolledRecordSink<String, String>, _: PartitionRegistry ->
                object : ConsumerPollLoopControl {
                    override fun start() = CoroutineScope(context).launch {
                        if (index == 0) {
                            failFirstLoop.await()
                            throw expected
                        }
                        try {
                            siblingStarted.complete(Unit)
                            awaitCancellation()
                        } finally {
                            siblingCancelled.complete(Unit)
                        }
                    }

                    override fun prepareForShutdown() = CompletableDeferred<Unit>()

                    override fun stateSnapshot(): PollLoopStateSnapshot =
                        PollLoopStateSnapshot(
                            id = 0,
                            started = true,
                            running = false,
                            shutdownRequested = false,
                            assignedPartitions = emptyList(),
                            lastPollEpochMillis = null,
                            lastPollRecordCount = null,
                            lastCommitAttemptEpochMillis = null,
                            lastCommitSucceeded = null
                        )
                }
            },
            processingRuntimeFactory = ::defaultProcessingRuntime
        )

        consumer.start()
        withTimeout(2_000) { siblingStarted.await() }
        failFirstLoop.complete(Unit)
        withTimeout(2_000) {
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                metrics.consumerFailures.firstOrNull()
            }
        }
        val thrown = assertThrows(IllegalStateException::class.java) {
            runBlocking {
                withTimeout(2_000) { consumer.stop() }
            }
        }

        withTimeout(2_000) { siblingCancelled.await() }
        assertEquals(expected.message, thrown.message)
        assertEquals(listOf(expected), metrics.consumerFailures)
    }

    @Test
    fun `when processing runtime fails then terminal shutdown returns its failure`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val expected = UnsupportedOperationException("failure handler failed")
        val consumer = createTestConsumer(
            records = listOf(typedTestRecord(offset = 1L, key = "key", value = "value")),
            consumerProperties = stringSerdeProperties(),
            metrics = metrics,
            processingFailureHandler = ProcessingFailureHandler { _, _ -> throw expected },
            handler = KafkaRecordHandler { throw IllegalStateException("processing failed") }
        )

        consumer.start()
        withTimeout(2_000) {
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                metrics.consumerFailures.firstOrNull()
            }
        }

        val thrown = assertThrows(UnsupportedOperationException::class.java) {
            runBlocking {
                withTimeout(2_000) { consumer.stop() }
            }
        }

        assertEquals(expected.message, thrown.message)
        assertEquals(1, metrics.consumerFailures.size)
        assertInstanceOf(UnsupportedOperationException::class.java, metrics.consumerFailures.single())
        assertEquals(expected.message, metrics.consumerFailures.single().message)
    }

    @Suppress("UNCHECKED_CAST")
    private fun createDefaultProcessingRuntime(processingMode: ProcessingMode) =
        defaultProcessingRuntime(
            parentScope = CoroutineScope(EmptyCoroutineContext),
            processingMode = processingMode,
            workerConcurrency = 1,
            workChannelCapacity = 16,
            freshnessMaxRecordAge = null,
            processingDispatcher = Dispatchers.Default,
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
            handler = KafkaRecordHandler { },
            retryPolicy = RetryPolicy.none(),
            recordProcessingContext = null,
            processingFailureHandler = ProcessingFailureHandler.skip(),
            processedRecordTracker = NoopProcessedRecordTracker
        )

    @Suppress("UNCHECKED_CAST")
    private fun lifecycleTestConsumer(
        consumerPollLoopConcurrency: Int = 1,
        processingDispatcher: CoroutineDispatcher = Dispatchers.Default,
        parentContext: CoroutineContext = Dispatchers.Default,
        pollLoopFactory: (Int, CoroutineContext) -> ConsumerPollLoopControl
    ): CoroutinesKafkaConsumer<String, String> = CoroutinesKafkaConsumer(
        processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        workerConcurrency = 1,
        consumerPollLoopConcurrency = consumerPollLoopConcurrency,
        commitIntervalMs = 1_000L,
        commitRecordsThreshold = 1_000,
        workChannelCapacity = 16,
        freshnessMaxRecordAge = null,
        processingDispatcher = processingDispatcher,
        consumerProperties = stringSerdeProperties(),
        handler = KafkaRecordHandler { },
        retryPolicy = RetryPolicy.none(),
        metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
        recordProcessingContext = null,
        processingFailureHandler = ProcessingFailureHandler.skip(),
        parentContext = parentContext,
        topics = listOf("topic-a"),
        topicsPattern = null,
        pollLoopFactory = { id, context, _, _, _, _, _, _, _, _, _, _ ->
            pollLoopFactory(id, context)
        },
        processingRuntimeFactory = ::defaultProcessingRuntime
    )

    private class TrackingPollLoop(
        private val parentContext: CoroutineContext,
        private val startFailure: Throwable? = null
    ) : ConsumerPollLoopControl {
        private val terminalFailure = CompletableDeferred<Throwable?>()
        val releaseCalls = AtomicInteger()
        var job: Job? = null
            private set

        override fun start(): Job {
            startFailure?.let { throw it }
            return CoroutineScope(parentContext).launch {
                terminalFailure.await()?.let { throw it }
            }.also { job = it }
        }

        override fun prepareForShutdown() = CompletableDeferred(Unit)

        override fun finalizeShutdown(): CompletableDeferred<Unit> {
            terminalFailure.complete(null)
            return CompletableDeferred(Unit)
        }

        override fun stateSnapshot(): PollLoopStateSnapshot = error("not needed by this test")

        override suspend fun releaseResources() {
            releaseCalls.incrementAndGet()
        }

        fun fail(error: Throwable) {
            terminalFailure.complete(error)
        }
    }
}
