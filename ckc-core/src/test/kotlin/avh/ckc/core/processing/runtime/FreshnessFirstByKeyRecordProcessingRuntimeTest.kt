package avh.ckc.core.processing.runtime

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.RetryPolicy
import avh.ckc.core.awaitFor
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.processing.NoopProcessedRecordTracker
import avh.ckc.core.typedTestRecord
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class FreshnessFirstByKeyRecordProcessingRuntimeTest {
    @Test
    fun `when same key record is already queued then newer record replaces it`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val metrics = RecordingMetrics<String, String>()
        val runtime = freshnessByKeyRuntime(
            workerConcurrency = 1,
            workChannelCapacity = 4,
            metrics = metrics,
            handler = KafkaRecordHandler { record ->
                if (record.offset() == 1L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                processedOffsets += record.offset()
            }
        )

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "hot-key")))
        withTimeout(2_000) { firstStarted.await() }

        assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, key = "hot-key")))
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 3L, key = "hot-key")))

        releaseFirst.complete(Unit)
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            processedOffsets.takeIf { it.size == 2 }
        }
        runtime.stop()

        assertEquals(listOf(1L, 3L), processedOffsets.toList())
        assertEquals(listOf(2L), metrics.dropped.map { it.record.offset() })
        assertEquals(
            listOf(RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD),
            metrics.dropped.map { it.reason }
        )
    }

    @Test
    fun `when queued key capacity is full then incoming new key record is dropped`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val metrics = RecordingMetrics<String, String>()
        val runtime = freshnessByKeyRuntime(
            workerConcurrency = 1,
            workChannelCapacity = 1,
            metrics = metrics,
            handler = KafkaRecordHandler { record ->
                if (record.offset() == 1L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                processedOffsets += record.offset()
            }
        )

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "key-a")))
        withTimeout(2_000) { firstStarted.await() }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, key = "key-b")))
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 3L, key = "key-c")))

        releaseFirst.complete(Unit)
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            processedOffsets.takeIf { it.size == 2 }
        }
        runtime.stop()

        assertEquals(listOf(1L, 2L), processedOffsets.toList())
        assertEquals(listOf(3L), metrics.dropped.map { it.record.offset() })
        assertEquals(listOf(RecordDropReason.NEW_KEY_QUEUE_FULL), metrics.dropped.map { it.reason })
    }

    @Test
    fun `when queued key capacity is full then existing queued key can still be replaced`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val metrics = RecordingMetrics<String, String>()
        val runtime = freshnessByKeyRuntime(
            workerConcurrency = 1,
            workChannelCapacity = 1,
            metrics = metrics,
            handler = KafkaRecordHandler { record ->
                if (record.offset() == 1L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                processedOffsets += record.offset()
            }
        )

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 1L, key = "key-a")))
        withTimeout(2_000) { firstStarted.await() }
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 2L, key = "key-b")))
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 3L, key = "key-b")))
        assertTrue(runtime.tryEmit(typedTestRecord(offset = 4L, key = "key-c")))

        releaseFirst.complete(Unit)
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            processedOffsets.takeIf { it.size == 2 }
        }
        runtime.stop()

        assertEquals(listOf(1L, 3L), processedOffsets.toList())
        assertEquals(listOf(2L, 4L), metrics.dropped.map { it.record.offset() })
        assertEquals(
            listOf(RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD, RecordDropReason.NEW_KEY_QUEUE_FULL),
            metrics.dropped.map { it.reason }
        )
    }

    @Test
    fun `when null key records are queued then they share one freshness lane`() = runBlocking {
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val metrics = RecordingMetrics<String, String>()
        val runtime = freshnessByKeyRuntime(
            workerConcurrency = 1,
            workChannelCapacity = 4,
            metrics = metrics,
            handler = KafkaRecordHandler { record ->
                if (record.offset() == 1L) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                processedOffsets += record.offset()
            }
        )

        runtime.start { throw it }
        assertTrue(runtime.tryEmit(nullableKeyRecord(offset = 1L)))
        withTimeout(2_000) { firstStarted.await() }
        assertTrue(runtime.tryEmit(nullableKeyRecord(offset = 2L)))
        assertTrue(runtime.tryEmit(nullableKeyRecord(offset = 3L)))

        releaseFirst.complete(Unit)
        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            processedOffsets.takeIf { it.size == 2 }
        }
        runtime.stop()

        assertEquals(listOf(1L, 3L), processedOffsets.toList())
        assertEquals(listOf(2L), metrics.dropped.map { it.record.offset() })
        assertEquals(
            listOf(RecordDropReason.REPLACED_BY_NEWER_KEY_RECORD),
            metrics.dropped.map { it.reason }
        )
    }

    @Test
    fun `when queued key record is older than max age then it is dropped before handler`() = runBlocking {
        val metrics = RecordingMetrics<String, String>()
        val processedOffsets = CopyOnWriteArrayList<Long>()
        val runtime = freshnessByKeyRuntime(
            workerConcurrency = 1,
            workChannelCapacity = 1,
            freshnessMaxRecordAge = 10.seconds,
            metrics = metrics,
            handler = KafkaRecordHandler { record ->
                processedOffsets += record.offset()
            }
        )

        runtime.start { throw it }
        assertTrue(
            runtime.tryEmit(
                typedTestRecord(
                    offset = 1L,
                    timestamp = System.currentTimeMillis() - 20_000L,
                    key = "key-1",
                    value = "value-1"
                )
            )
        )

        awaitFor(timeoutMillis = 2_000, pauseMillis = 10) {
            metrics.dropped.singleOrNull()
        }
        runtime.stop()

        assertEquals(emptyList<Long>(), processedOffsets.toList())
        assertEquals(listOf(1L), metrics.dropped.map { it.record.offset() })
        assertEquals(listOf(RecordDropReason.STALE_AGE), metrics.dropped.map { it.reason })
    }

    private fun freshnessByKeyRuntime(
        workerConcurrency: Int,
        workChannelCapacity: Int,
        freshnessMaxRecordAge: Duration? = null,
        metrics: ConsumerMetrics<String, String>,
        handler: KafkaRecordHandler<String, String>
    ): FreshnessFirstByKeyRecordProcessingRuntime<String, String> =
        FreshnessFirstByKeyRecordProcessingRuntime(
            workerConcurrency = workerConcurrency,
            workChannelCapacity = workChannelCapacity,
            freshnessMaxRecordAge = freshnessMaxRecordAge,
            processingDispatcher = Dispatchers.Default,
            scope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
            metrics = metrics,
            handler = handler,
            retryPolicy = RetryPolicy.none(),
            processingFailureHandler = ProcessingFailureHandler.skip(),
            processedRecordTracker = NoopProcessedRecordTracker
        )

    private fun nullableKeyRecord(offset: Long): ConsumerRecord<String, String> =
        ConsumerRecord("topic-a", 0, offset, null, "value-$offset")
}
