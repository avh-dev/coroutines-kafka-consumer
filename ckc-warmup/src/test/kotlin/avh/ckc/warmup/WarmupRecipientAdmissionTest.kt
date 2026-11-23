package avh.ckc.warmup

import avh.ckc.core.KafkaRecordHandler
import avh.ckc.core.ProcessingMode
import avh.ckc.core.RecordProcessingContext
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.createTestConsumer
import avh.ckc.core.stringSerdeProperties
import avh.ckc.core.testRuntime
import avh.ckc.warmup.protocol.WARMUP_SESSION_HEADER
import avh.ckc.warmup.protocol.WarmupSessionId
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WarmupRecipientAdmissionTest {
    @Test
    fun `only one exact session header is admitted`() {
        val sessionId = WarmupSessionId("recipient")
        val admission = WarmupRecipientAdmission<String, String>(sessionId)
        val records = listOf(
            record(0, sessionId.value),
            record(1, "other"),
            record(2),
            record(3, sessionId.value, sessionId.value),
            record(4, headerValues = listOf(byteArrayOf(0xC3.toByte(), 0x28))),
            record(5, headerValues = listOf(null))
        )

        assertEquals(listOf(0L), records.filterNot(admission::shouldSkip).map { it.offset() })
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
                recordAdmission = WarmupRecipientAdmission(sessionId),
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

}
