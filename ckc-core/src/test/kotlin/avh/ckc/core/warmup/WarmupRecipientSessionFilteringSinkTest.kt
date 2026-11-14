package avh.ckc.core.warmup

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingFailureHandler
import avh.ckc.core.ProcessingMode
import avh.ckc.core.RecordProcessingContext
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.RetryPolicy
import avh.ckc.core.createTestConsumer
import avh.ckc.core.defaultProcessingRuntime
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.stringSerdeProperties
import avh.ckc.core.testRuntime
import avh.ckc.core.isFreshnessFirst
import avh.ckc.core.tracksProcessedOffsets
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

class WarmupRecipientSessionFilteringSinkTest {
    @Test
    fun `foreign record is not passed to a backpressured delegate`() {
        val sessionId = WarmupSessionId("recipient")
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val delegateCalls = mutableListOf<Long>()
        val sink = WarmupRecipientSessionFilteringSink(
            sessionId,
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean {
                    delegateCalls += record.offset()
                    return false
                }
            },
            tracker
        )
        val own = record(0, sessionId.value)
        val foreign = record(1, "other")
        assertTrue(registry.observe(own) != null)
        assertTrue(registry.observe(foreign) != null)

        assertFalse(sink.tryEmit(own))
        assertTrue(sink.tryEmit(foreign))

        assertEquals(listOf(0L), delegateCalls)
        assertTrue(tracker.tryStart(own))
        tracker.finish(own)
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `foreign record cannot displace an own record from a full runtime queue`() = runBlocking {
        listOf(
            ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
        ).forEach { mode ->
            val sessionId = WarmupSessionId("recipient-$mode")
            val registry = assignedRegistry()
            val tracker = PartitionProcessedRecordTracker(registry, mode.tracksProcessedOffsets())
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val firstStarted = CompletableDeferred<Unit>()
            val releaseFirst = CompletableDeferred<Unit>()
            val handledOffsets = CopyOnWriteArrayList<Long>()
            val failure = CompletableDeferred<Throwable>()
            val runtime = defaultProcessingRuntime(
                parentScope = scope,
                processingMode = mode,
                workerConcurrency = 1,
                workChannelCapacity = 1,
                freshnessMaxRecordAge = null,
                processingDispatcher = Dispatchers.Default,
                metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
                handler = KafkaRecordHandler { record ->
                    if (record.offset() == 0L) {
                        firstStarted.complete(Unit)
                        releaseFirst.await()
                    }
                    handledOffsets += record.offset()
                },
                retryPolicy = RetryPolicy.none(),
                recordProcessingContext = null,
                processingFailureHandler = ProcessingFailureHandler.skip(),
                processedRecordTracker = tracker
            )
            val sink = WarmupRecipientSessionFilteringSink(sessionId, runtime, tracker)
            val ownInFlight = record(0, sessionId.value, key = "shared")
            val ownPending = record(1, sessionId.value, key = "shared")
            val foreign = record(2, "other", key = "shared")

            runtime.start { failure.complete(it) }
            try {
                listOf(ownInFlight, ownPending, foreign).forEach { record ->
                    assertTrue(
                        registry.observe(
                            record,
                            observeOffset = mode.tracksProcessedOffsets(),
                            acceptOffset = mode.isFreshnessFirst()
                        ) != null,
                        "mode $mode failed to observe offset ${record.offset()}"
                    )
                    if (record === ownInFlight) {
                        assertTrue(sink.tryEmit(record), "mode $mode")
                        withTimeout(2_000) { firstStarted.await() }
                    } else {
                        assertTrue(sink.tryEmit(record), "mode $mode offset ${record.offset()}")
                    }
                }

                assertEquals(1, runtime.stateSnapshot().workQueueSize, "mode $mode")
                releaseFirst.complete(Unit)
                withTimeout(2_000) {
                    while (handledOffsets.size != 2) kotlinx.coroutines.yield()
                }

                assertEquals(listOf(0L, 1L), handledOffsets, "mode $mode")
                assertFalse(failure.isCompleted, "mode $mode")
            } finally {
                releaseFirst.complete(Unit)
                runtime.stop()
                scope.cancel()
            }
        }
    }

    @Test
    fun `interleaved foreign offsets cannot advance past unfinished recipient records`() {
        val sessionId = WarmupSessionId("recipient")
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val admitted = mutableListOf<ConsumerRecord<String, String>>()
        val sink = WarmupRecipientSessionFilteringSink(
            sessionId,
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean {
                    admitted += record
                    return true
                }
            },
            tracker
        )
        val ownFirst = record(0, sessionId.value)
        val foreign = record(1, "other")
        val ownLater = record(2, sessionId.value)

        listOf(ownFirst, foreign, ownLater).forEach {
            assertTrue(registry.observe(it) != null)
            assertTrue(sink.tryEmit(it))
        }

        assertEquals(listOf(0L, 2L), admitted.map(ConsumerRecord<String, String>::offset))
        assertNull(registry.partitionStateFor(ownFirst)?.advanceAndGetCommitData())
        tracker.markProcessed(ownLater)
        tracker.discard(ownLater)
        assertNull(registry.partitionStateFor(ownFirst)?.advanceAndGetCommitData())

        tracker.markProcessed(ownFirst)
        tracker.discard(ownFirst)
        assertEquals(2L, registry.partitionStateFor(ownFirst)?.advanceAndGetCommitData()?.offset)
        assertFalse(registry.tryStart(foreign))
    }

    @Test
    fun `only one exact session header is admitted`() {
        val sessionId = WarmupSessionId("recipient")
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val admittedOffsets = mutableListOf<Long>()
        val sink = WarmupRecipientSessionFilteringSink(
            sessionId,
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean {
                    admittedOffsets += record.offset()
                    return true
                }
            },
            tracker
        )
        val records = listOf(
            record(0, sessionId.value),
            record(1, "other"),
            record(2),
            record(3, sessionId.value, sessionId.value),
            record(4, headerValues = listOf(byteArrayOf(0xC3.toByte(), 0x28))),
            record(5, headerValues = listOf(null))
        )

        records.forEach {
            assertTrue(registry.observe(it) != null)
            assertTrue(sink.tryEmit(it))
        }

        assertEquals(listOf(0L), admittedOffsets)
        assertTrue(records.drop(1).none(registry::tryStart))
    }

    @Test
    fun `completion after ownership generation changes cannot advance the new owner`() {
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val record = record(0, "foreign")
        assertTrue(registry.observe(record) != null)
        registry.revoke(listOf(PARTITION))
        registry.onPartitionsAssigned(listOf(PARTITION))
        val sink = WarmupRecipientSessionFilteringSink(
            WarmupSessionId("recipient"),
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean =
                    error("foreign record must not be delegated")
            },
            tracker
        )

        assertTrue(sink.tryEmit(record))

        assertNull(registry.partitionStateFor(PARTITION)?.advanceAndGetCommitData())
        assertFalse(registry.tryStart(record))
    }

    @Test
    fun `foreign records bypass every processing mode and ordinary terminal observations`() = runBlocking {
        ProcessingMode.entries.forEach { mode ->
            val sessionId = WarmupSessionId("recipient-$mode")
            val records = listOf(
                record(0, "foreign"),
                record(1, sessionId.value),
                record(2),
                record(3, sessionId.value, sessionId.value)
            )
            val metrics = RecordingMetrics<String, String>()
            val handledOffsets = CopyOnWriteArrayList<Long>()
            val contextCalls = AtomicInteger()
            val consumer = createTestConsumer(
                records = records,
                consumerProperties = stringSerdeProperties(),
                handler = KafkaRecordHandler { handledOffsets += it.offset() },
                metrics = metrics,
                recordProcessingContext = RecordProcessingContext { _, block ->
                    contextCalls.incrementAndGet()
                    block()
                },
                warmupRecipientSessionId = sessionId,
                runtime = testRuntime(processingMode = mode)
            )

            consumer.start()
            withTimeout(2_000) {
                while (metrics.processed.size != 1) kotlinx.coroutines.yield()
            }
            consumer.stop()

            assertEquals(listOf(1L), handledOffsets, "mode $mode")
            assertEquals(1, contextCalls.get(), "mode $mode")
            assertEquals(listOf(1L), metrics.processed.map { it.record.offset() }, "mode $mode")
            assertEquals(listOf(records.size), metrics.polls.map { it.first }, "mode $mode")
            assertTrue(metrics.failed.isEmpty(), "mode $mode")
            assertTrue(metrics.dropped.isEmpty(), "mode $mode")
            assertTrue(metrics.retries.isEmpty(), "mode $mode")
            assertTrue(metrics.handedOff.isEmpty(), "mode $mode")
            assertEquals(1, consumer.stateSnapshot().processing.maxObservedWorkQueueSize, "mode $mode")
        }
    }

    private fun assignedRegistry(): PartitionRegistry = PartitionRegistry().also { registry ->
        registry.onPartitionsAssigned(listOf(PARTITION)).single().init(0L)
    }

    private fun record(
        offset: Long,
        vararg sessions: String,
        key: String = "key-$offset"
    ): ConsumerRecord<String, String> =
        record(offset, sessions.map { it.toByteArray(Charsets.UTF_8) }, key)

    private fun record(
        offset: Long,
        headerValues: List<ByteArray?>,
        key: String = "key-$offset"
    ): ConsumerRecord<String, String> = ConsumerRecord("warmup", 0, offset, key, "value-$offset").also {
        headerValues.forEach { value -> it.headers().add(WARMUP_SESSION_HEADER, value) }
    }

    private companion object {
        val PARTITION = TopicPartition("warmup", 0)
    }
}
