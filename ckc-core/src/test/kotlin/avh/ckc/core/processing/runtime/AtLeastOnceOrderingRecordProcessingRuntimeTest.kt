package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.RetryPolicy
import avh.ckc.core.RetryRule
import avh.ckc.core.awaitFor
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.typedTestRecord
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.TimeoutCancellationException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class AtLeastOnceOrderingRecordProcessingRuntimeTest {
    @Test
    fun `when revoke follows failed attempt then next retry is not started or marked processed`() = runBlocking {
        val registry = PartitionRegistry()
        val topicPartition = TopicPartition("topic", 0)
        val state = registry.onPartitionsAssigned(listOf(topicPartition)).single().also { it.init(0L) }
        val record = typedTestRecord(topic = topicPartition.topic(), offset = 0L, key = "key-a")
        registry.observe(record)
        val attemptStarted = CompletableDeferred<Unit>()
        val releaseAttempt = CompletableDeferred<Unit>()
        val runtimeFailure = CompletableDeferred<Throwable>()
        val attempts = AtomicInteger()
        val failureHandlerCalls = AtomicInteger()
        val metrics = RecordingMetrics<String, String>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            processedRecordTracker = PartitionProcessedRecordTracker(registry),
            retryPolicy = RetryPolicy.of(RetryRule.of<IOException>(maxRetries = 3, delay = 30.seconds)),
            processingFailureHandler = ProcessingFailureHandler { _, _ -> failureHandlerCalls.incrementAndGet() },
            metrics = metrics,
            handler = KafkaRecordHandler {
                attempts.incrementAndGet()
                attemptStarted.complete(Unit)
                releaseAttempt.await()
                throw IOException("transient")
            }
        )
        runtime.start { runtimeFailure.complete(it) }
        assertTrue(runtime.tryEmit(record))
        withTimeout(2_000) { attemptStarted.await() }

        registry.revoke(listOf(topicPartition))
        releaseAttempt.complete(Unit)

        assertTrue(state.awaitPipelines(System.nanoTime() + 2.seconds.inWholeNanoseconds))
        assertEquals(1, attempts.get())
        assertEquals(0, failureHandlerCalls.get())
        assertFalse(runtimeFailure.isCompleted)
        assertEquals(listOf(RecordHandoffReason.ASSIGNMENT_ENDED), metrics.handedOff.map { it.reason })
        assertTrue(metrics.dropped.isEmpty())
        assertEquals(null, state.advanceAndGetCommitData())
        runtime.stop()
    }

    @Test
    fun `when consumer fails then cancelled tracked record is handed off instead of dropped`() = runBlocking {
        val registry = PartitionRegistry()
        val topicPartition = TopicPartition("topic", 0)
        registry.onPartitionsAssigned(listOf(topicPartition)).single().init(0L)
        val record = typedTestRecord(topic = topicPartition.topic(), offset = 0L, key = "key-a")
        registry.observe(record)
        val entered = CompletableDeferred<Unit>()
        val metrics = RecordingMetrics<String, String>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            processedRecordTracker = PartitionProcessedRecordTracker(registry),
            metrics = metrics,
            handler = KafkaRecordHandler {
                entered.complete(Unit)
                awaitCancellation()
            }
        )
        runtime.start { throw it }
        assertTrue(runtime.tryEmit(record))
        withTimeout(2_000) { entered.await() }

        registry.cancelAllActive()

        awaitFor(2_000L, 10L) { metrics.handedOff.firstOrNull() }
        assertEquals(listOf(RecordHandoffReason.CONSUMER_FAILED), metrics.handedOff.map { it.reason })
        assertTrue(metrics.dropped.isEmpty())
        runtime.stop()
    }

    @Test
    fun `when handler fails during graceful drain then runtime failure is reported`() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val failure = CompletableDeferred<Throwable>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            handler = KafkaRecordHandler {
                entered.complete(Unit)
                release.await()
                throw CancellationException("failed while draining")
            }
        )
        runtime.start { failure.complete(it) }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "key-a")))
        withTimeout(2_000) { entered.await() }

        // UNDISPATCHED runs stop() through its first suspension, which closes admission and
        // establishes that the handler failure below happens during the graceful drain.
        val stop = launch(start = CoroutineStart.UNDISPATCHED) { runtime.stop() }
        release.complete(Unit)

        assertEquals("failed while draining", withTimeout(2_000) { failure.await() }.message)
        withTimeout(2_000) { stop.join() }
    }

    @Test
    fun `when handler times out then failure is not treated as lifecycle cancellation`() = runBlocking {
        val failure = CompletableDeferred<Throwable>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            handler = KafkaRecordHandler {
                withTimeout(1) { delay(1_000) }
            }
        )
        runtime.start { failure.complete(it) }

        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "key-a")))

        assertTrue(withTimeout(2_000) { failure.await() } is TimeoutCancellationException)
        runtime.stop()
    }

    @Test
    fun `when ordered by key then records with same key are processed sequentially in poll order`() = runBlocking {
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val activeByKey = ConcurrentHashMap<String, AtomicInteger>()
        val maxActiveByKey = ConcurrentHashMap<String, AtomicInteger>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 4,
            workChannelCapacity = 16,
            handler = KafkaRecordHandler<String, String> { record ->
                val key = record.key()
                val active = activeByKey.computeIfAbsent(key!!) { AtomicInteger() }.incrementAndGet()
                maxActiveByKey.computeIfAbsent(key) { AtomicInteger() }.updateAndGet { maxOf(it, active) }
                delay(10)
                processedOffsets += record.offset()
                activeByKey[key]!!.decrementAndGet()
            }
        )

        runtime.start { throw it }
        repeat(8) { index ->
            assertTrue(runtime.tryEmit(typedTestRecord(offset = index.toLong(), key = "same-key")))
        }

        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            processedOffsets.takeIf { it.size == 8 }
        }
        runtime.stop()

        assertEquals((0L..7L).toList(), processedOffsets.toList())
        assertEquals(1, maxActiveByKey["same-key"]!!.get())
    }

    @Test
    fun `when ordered by key then records with different keys may process concurrently`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 2,
            workChannelCapacity = 8,
            handler = KafkaRecordHandler<String, String> { record ->
                val key = record.key()
                if (key == "key-a") {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                } else if (key == "key-b") {
                    secondStarted.complete(Unit)
                }
            }
        )

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "key-a")))
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, key = "key-b")))

        withTimeout(2_000) { firstStarted.await() }
        withTimeout(2_000) { secondStarted.await() }
        releaseFirst.complete(Unit)
        runtime.stop()
    }

    @Test
    fun `when ordered by key then equal byte array keys are sequential even if handler mutates a key`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val runtime = byteArrayOrderedRuntime { record ->
            if (record.offset() == 1L) {
                record.key()[0] = 99
                firstStarted.complete(Unit)
                releaseFirst.await()
            } else {
                secondStarted.complete(Unit)
            }
        }

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(ConsumerRecord("topic-a", 0, 1L, byteArrayOf(1, 2), "first")))
        assertTrue(runtime.tryEmit(ConsumerRecord("topic-a", 0, 2L, byteArrayOf(1, 2), "second")))

        withTimeout(2_000) { firstStarted.await() }
        assertEquals(null, withTimeoutOrNull(200) { secondStarted.await() })
        releaseFirst.complete(Unit)
        withTimeout(2_000) { secondStarted.await() }
        runtime.stop()
    }

    @Test
    fun `when ordered by key then different byte array content may process concurrently`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val runtime = byteArrayOrderedRuntime { record ->
            if (record.offset() == 1L) {
                firstStarted.complete(Unit)
                releaseFirst.await()
            } else {
                secondStarted.complete(Unit)
            }
        }

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(ConsumerRecord("topic-a", 0, 1L, byteArrayOf(1, 2), "first")))
        assertTrue(runtime.tryEmit(ConsumerRecord("topic-a", 0, 2L, byteArrayOf(1, 3), "second")))

        withTimeout(2_000) { firstStarted.await() }
        withTimeout(2_000) { secondStarted.await() }
        releaseFirst.complete(Unit)
        runtime.stop()
    }

    @Test
    fun `when ordered by partition then same partition is sequential and different partitions may process concurrently`() =
        runBlocking {
            val firstPartitionRecordStarted = CompletableDeferred<Unit>()
            val otherPartitionRecordStarted = CompletableDeferred<Unit>()
            val releaseFirstPartitionRecord = CompletableDeferred<Unit>()
            val processedOffsets = CopyOnWriteArrayList<Long>()
            val runtime = orderedRuntime(
                ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_PARTITION,
                workerConcurrency = 2,
                workChannelCapacity = 8,
                handler = KafkaRecordHandler<String, String> { record ->
                    when (record.offset()) {
                        1L -> {
                            firstPartitionRecordStarted.complete(Unit)
                            releaseFirstPartitionRecord.await()
                        }
                        3L -> otherPartitionRecordStarted.complete(Unit)
                    }
                    processedOffsets += record.offset()
                }
            )

            runtime.start { throw it }
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, partition = 0, key = "key-a")))
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, partition = 0, key = "key-b")))
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 3L, partition = 1, key = "key-c")))

            withTimeout(2_000) { firstPartitionRecordStarted.await() }
            withTimeout(2_000) { otherPartitionRecordStarted.await() }
            assertFalse(processedOffsets.contains(2L))

            releaseFirstPartitionRecord.complete(Unit)
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                processedOffsets.takeIf { it.containsAll(listOf(1L, 2L, 3L)) }
            }
            runtime.stop()

            assertTrue(processedOffsets.indexOf(1L) < processedOffsets.indexOf(2L))
        }

    @Test
    fun `when accepted records fill admission budget then tryEmit returns false until processing releases capacity`() =
        runBlocking {
            val firstStarted = CompletableDeferred<Unit>()
            val releaseFirst = CompletableDeferred<Unit>()
            val processedOffsets = CopyOnWriteArrayList<Long>()
            val runtime = orderedRuntime(
                ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
                workerConcurrency = 1,
                workChannelCapacity = 2,
                handler = KafkaRecordHandler<String, String> { record ->
                    if (record.offset() == 1L) {
                        firstStarted.complete(Unit)
                        releaseFirst.await()
                    }
                    processedOffsets += record.offset()
                }
            )

            runtime.start { throw it }
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "hot-key")))
            assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, key = "hot-key")))
            withTimeout(2_000) { firstStarted.await() }

            assertFalse(runtime.tryEmit(typedTestRecord(offset = 3L, key = "hot-key")))

            releaseFirst.complete(Unit)
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                processedOffsets.takeIf { it.contains(1L) }
            }
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                runtime.tryEmit(typedTestRecord(offset = 3L, key = "hot-key")).takeIf { it }
            }
            awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
                processedOffsets.takeIf { it.containsAll(listOf(1L, 2L, 3L)) }
            }
            runtime.stop()
        }

    @Test
    fun `when records wait behind hot key then ordering queue stats expose current and maximum contention`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val metrics = RecordingMetrics<String, String>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            metrics = metrics,
            handler = KafkaRecordHandler { record ->
                if (record.offset() == 1L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
            }
        )

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "hot-key")))
        withTimeout(2_000) { firstStarted.await() }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, key = "hot-key")))
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 3L, key = "hot-key")))

        val stats = metrics.boundRuntimeStats.single()
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            stats.takeIf { it.orderingQueueSize == 2 }
        }
        assertEquals(2, stats.maxObservedOrderingQueueSize)

        releaseFirst.complete(Unit)
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            stats.takeIf { it.orderingQueueSize == 0 && it.workQueueSize == 0 }
        }
        runtime.stop()

        assertEquals(2, stats.maxObservedOrderingQueueSize)
    }

    @Test
    fun `when record is already processed then it is reported as dropped without processing`() {
        val metrics = RecordingMetrics<String, String>()
        val handledRecords = CopyOnWriteArrayList<Long>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            workerConcurrency = 1,
            workChannelCapacity = 4,
            metrics = metrics,
            processedRecordTracker = object : ProcessedRecordTracker {
                override fun <K, V> markProcessed(record: ConsumerRecord<K, V>) = Unit

                override fun <K, V> isProcessed(record: ConsumerRecord<K, V>): Boolean =
                    record.offset() == 43L
            },
            handler = KafkaRecordHandler { record ->
                handledRecords += record.offset()
            }
        )

        assertTrue(runtime.tryEmit(typedTestRecord(offset = 43L, key = "key-a")))

        assertTrue(handledRecords.isEmpty())
        assertEquals(listOf(43L), metrics.dropped.map { it.record.offset() })
        assertEquals(listOf(RecordDropReason.ALREADY_PROCESSED), metrics.dropped.map { it.reason })
    }

    private fun orderedRuntime(
        ordering: AtLeastOnceOrderingRecordProcessingRuntime.Ordering,
        workerConcurrency: Int,
        workChannelCapacity: Int,
        metrics: ConsumerMetrics<String, String> = noopMetrics(),
        processedRecordTracker: ProcessedRecordTracker = NoopProcessedRecordTracker,
        retryPolicy: RetryPolicy = RetryPolicy.none(),
        processingFailureHandler: ProcessingFailureHandler<String, String> = ProcessingFailureHandler.skip(),
        handler: KafkaRecordHandler<String, String>
    ): AtLeastOnceOrderingRecordProcessingRuntime<String, String> {
        return AtLeastOnceOrderingRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            ordering = ordering,
            processingDispatcher = Dispatchers.Default,
            scope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
            metrics = metrics,
            handler = handler,
            retryPolicy = retryPolicy,
            processingFailureHandler = processingFailureHandler,
            recordProcessingContext = null,
            processedRecordTracker = processedRecordTracker
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun noopMetrics(): ConsumerMetrics<String, String> =
        ConsumerMetrics.NOOP as ConsumerMetrics<String, String>

    @Suppress("UNCHECKED_CAST")
    private fun byteArrayOrderedRuntime(
        handler: KafkaRecordHandler<ByteArray, String>
    ): AtLeastOnceOrderingRecordProcessingRuntime<ByteArray, String> =
        AtLeastOnceOrderingRecordProcessingRuntime(
            workerConcurrency = 2,
            workChannelCapacity = 8,
            ordering = AtLeastOnceOrderingRecordProcessingRuntime.Ordering.BY_KEY,
            processingDispatcher = Dispatchers.Default,
            scope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<ByteArray, String>,
            handler = handler,
            retryPolicy = RetryPolicy.none(),
            processingFailureHandler = ProcessingFailureHandler.skip(),
            recordProcessingContext = null,
            processedRecordTracker = NoopProcessedRecordTracker
        )
}
