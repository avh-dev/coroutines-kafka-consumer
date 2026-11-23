package avh.ckc.core

import avh.ckc.core.extension.RecordAdmission
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.processing.AdmissionControlledRecordSink
import avh.ckc.core.processing.PartitionProcessedRecordTracker
import avh.ckc.core.processing.PolledRecordSink
import java.util.concurrent.CopyOnWriteArrayList
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

class AdmissionControlledRecordSinkTest {
    @Test
    fun `capacity is reserved before queue admission and released when runtime rejects the record`() {
        val sessionId = "recipient"
        val tracker = PartitionProcessedRecordTracker(assignedRegistry())
        val events = mutableListOf<String>()
        val admission = object : RecordAdmission<String, String> {
            override fun shouldSkip(record: ConsumerRecord<String, String>): Boolean = false

            override fun tryAcquire(record: ConsumerRecord<String, String>): Boolean {
                events += "acquire"
                return true
            }

            override fun release(record: ConsumerRecord<String, String>) {
                events += "release"
            }
        }
        val sink = admissionSink(
            sessionId,
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean {
                    events += "delegate"
                    return false
                }
            },
            tracker,
            admission
        )

        assertFalse(sink.tryEmit(record(0, sessionId)))
        assertEquals(listOf("acquire", "delegate", "release"), events)
    }

    @Test
    fun `foreign record is not passed to a backpressured delegate`() {
        val sessionId = "recipient"
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val delegateCalls = mutableListOf<Long>()
        val sink = admissionSink(
            sessionId,
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean {
                    delegateCalls += record.offset()
                    return false
                }
            },
            tracker
        )
        val own = record(0, sessionId)
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
            val sessionId = "recipient-$mode"
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
            val sink = admissionSink(sessionId, runtime, tracker)
            val ownInFlight = record(0, sessionId, key = "shared")
            val ownPending = record(1, sessionId, key = "shared")
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
        val sessionId = "recipient"
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val admitted = mutableListOf<ConsumerRecord<String, String>>()
        val sink = admissionSink(
            sessionId,
            object : PolledRecordSink<String, String> {
                override fun tryEmit(record: ConsumerRecord<String, String>): Boolean {
                    admitted += record
                    return true
                }
            },
            tracker
        )
        val ownFirst = record(0, sessionId)
        val foreign = record(1, "other")
        val ownLater = record(2, sessionId)

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
    fun `completion after ownership generation changes cannot advance the new owner`() {
        val registry = assignedRegistry()
        val tracker = PartitionProcessedRecordTracker(registry)
        val record = record(0, "foreign")
        assertTrue(registry.observe(record) != null)
        registry.revoke(listOf(PARTITION))
        registry.onPartitionsAssigned(listOf(PARTITION))
        val sink = admissionSink(
            "recipient",
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

    private fun admissionSink(
        recordSet: String,
        delegate: PolledRecordSink<String, String>,
        tracker: PartitionProcessedRecordTracker,
        reservation: RecordAdmission<String, String>? = null
    ): PolledRecordSink<String, String> = AdmissionControlledRecordSink(
        delegate,
        tracker,
        reservation ?: RecordAdmission { record ->
            record.headers().lastHeader("record-set")?.value()?.toString(Charsets.UTF_8) != recordSet
        }
    )

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
    ): ConsumerRecord<String, String> = ConsumerRecord("topic", 0, offset, key, "value-$offset").also {
        headerValues.forEach { value -> it.headers().add("record-set", value) }
    }

    private companion object {
        val PARTITION = TopicPartition("topic", 0)
    }
}
