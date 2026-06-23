package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.RetryPolicy
import avh.ckc.core.awaitFor
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.processing.ProcessedRecordTracker
import avh.ckc.core.typedTestRecord
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

class AtLeastOnceOrderedRecordProcessingRuntimeTest {
    @Test
    fun `when ordered by key then records with same key are processed sequentially in poll order`() = runBlocking {
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val activeByKey = ConcurrentHashMap<String, AtomicInteger>()
        val maxActiveByKey = ConcurrentHashMap<String, AtomicInteger>()
        val runtime = orderedRuntime(
            ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_KEY,
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
            ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_KEY,
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
    fun `when ordered by partition then same partition is sequential and different partitions may process concurrently`() =
        runBlocking {
            val firstPartitionRecordStarted = CompletableDeferred<Unit>()
            val otherPartitionRecordStarted = CompletableDeferred<Unit>()
            val releaseFirstPartitionRecord = CompletableDeferred<Unit>()
            val processedOffsets = CopyOnWriteArrayList<Long>()
            val runtime = orderedRuntime(
                ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_PARTITION,
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
                ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_KEY,
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
            ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_KEY,
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
            ordering = AtLeastOnceOrderedRecordProcessingRuntime.Ordering.BY_KEY,
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
        ordering: AtLeastOnceOrderedRecordProcessingRuntime.Ordering,
        workerConcurrency: Int,
        workChannelCapacity: Int,
        metrics: ConsumerMetrics<String, String> = noopMetrics(),
        processedRecordTracker: ProcessedRecordTracker = NoopProcessedRecordTracker,
        handler: KafkaRecordHandler<String, String>
    ): AtLeastOnceOrderedRecordProcessingRuntime<String, String> {
        return AtLeastOnceOrderedRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            ordering = ordering,
            processingDispatcher = Dispatchers.Default,
            scope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
            metrics = metrics,
            handler = handler,
            retryPolicy = RetryPolicy.none(),
            processingFailureHandler = ProcessingFailureHandler.skip(),
            processedRecordTracker = processedRecordTracker
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun noopMetrics(): ConsumerMetrics<String, String> =
        ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
}
